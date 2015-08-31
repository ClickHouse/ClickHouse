#include <iomanip>
#include <thread>
#include <future>

#include <cxxabi.h>

#include <statdaemons/Stopwatch.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/AggregateFunctions/AggregateFunctionCount.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{


AggregatedDataVariants::~AggregatedDataVariants()
{
	if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
	{
		try
		{
			aggregator->destroyAllAggregateStates(*this);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
}


void AggregatedDataVariants::convertToTwoLevel()
{
	if (aggregator)
		LOG_TRACE(aggregator->log, "Converting aggregation data to two-level.");

	switch (type)
	{
	#define M(NAME) \
		case Type::NAME: \
			NAME ## _two_level.reset(new decltype(NAME ## _two_level)::element_type(*NAME)); \
			NAME.reset(); \
			type = Type::NAME ## _two_level; \
			break;

		APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

	#undef M

		default:
			throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
	}
}


void Aggregator::initialize(Block & block)
{
	if (isCancelled())
		return;

	std::lock_guard<std::mutex> lock(mutex);

	if (initialized)
		return;

	initialized = true;

	aggregate_functions.resize(aggregates_size);
	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_functions[i] = &*aggregates[i].function;

	/// Инициализируем размеры состояний и смещения для агрегатных функций.
	offsets_of_aggregate_states.resize(aggregates_size);
	total_size_of_aggregate_states = 0;
	all_aggregates_has_trivial_destructor = true;

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		offsets_of_aggregate_states[i] = total_size_of_aggregate_states;
		total_size_of_aggregate_states += aggregates[i].function->sizeOfData();

		if (!aggregates[i].function->hasTrivialDestructor())
			all_aggregates_has_trivial_destructor = false;
	}

	if (isCancelled())
		return;

	/** Всё остальное - только если передан непустой block.
	  * (всё остальное не нужно в методе merge блоков с готовыми состояниями агрегатных функций).
	  */
	if (!block)
		return;

	/// Преобразуем имена столбцов в номера, если номера не заданы
	if (keys.empty() && !key_names.empty())
		for (Names::const_iterator it = key_names.begin(); it != key_names.end(); ++it)
			keys.push_back(block.getPositionByName(*it));

	for (AggregateDescriptions::iterator it = aggregates.begin(); it != aggregates.end(); ++it)
		if (it->arguments.empty() && !it->argument_names.empty())
			for (Names::const_iterator jt = it->argument_names.begin(); jt != it->argument_names.end(); ++jt)
				it->arguments.push_back(block.getPositionByName(*jt));

	if (isCancelled())
		return;

	/// Создадим пример блока, описывающего результат
	if (!sample)
	{
		for (size_t i = 0; i < keys_size; ++i)
		{
			sample.insert(block.getByPosition(keys[i]).cloneEmpty());
			if (sample.getByPosition(i).column->isConst())
				sample.getByPosition(i).column = dynamic_cast<IColumnConst &>(*sample.getByPosition(i).column).convertToFullColumn();
		}

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			ColumnWithTypeAndName col;
			col.name = aggregates[i].column_name;

			size_t arguments_size = aggregates[i].arguments.size();
			DataTypes argument_types(arguments_size);
			for (size_t j = 0; j < arguments_size; ++j)
				argument_types[j] = block.getByPosition(aggregates[i].arguments[j]).type;

			col.type = new DataTypeAggregateFunction(aggregates[i].function, argument_types, aggregates[i].parameters);
			col.column = col.type->createColumn();

			sample.insert(col);
		}
	}
}


void Aggregator::compileIfPossible(AggregatedDataVariants::Type type)
{
	std::lock_guard<std::mutex> lock(mutex);

	if (compiled_if_possible)
		return;

	compiled_if_possible = true;

	std::string method_typename;
	std::string method_typename_two_level;

	if (false) {}
#define M(NAME) \
	else if (type == AggregatedDataVariants::Type::NAME) \
	{ \
		method_typename = "decltype(AggregatedDataVariants::" #NAME ")::element_type"; \
		method_typename_two_level = "decltype(AggregatedDataVariants::" #NAME "_two_level)::element_type"; \
	}

	APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M

#define M(NAME) \
	else if (type == AggregatedDataVariants::Type::NAME) \
		method_typename = "decltype(AggregatedDataVariants::" #NAME ")::element_type";

	APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
	else if (type == AggregatedDataVariants::Type::without_key) {}
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	/// Список типов агрегатных функций.
	std::stringstream aggregate_functions_typenames_str;
	for (size_t i = 0; i < aggregates_size; ++i)
	{
		IAggregateFunction & func = *aggregate_functions[i];

		int status = 0;
		char * type_name_ptr = abi::__cxa_demangle(typeid(func).name(), 0, 0, &status);
		std::string type_name = type_name_ptr;
		free(type_name_ptr);

		if (status)
			throw Exception("Cannot compile code: cannot demangle name " + String(typeid(func).name())
				+ ", status: " + toString(status), ErrorCodes::CANNOT_COMPILE_CODE);

		aggregate_functions_typenames_str << ((i != 0) ? ", " : "") << type_name;
	}

	std::string aggregate_functions_typenames = aggregate_functions_typenames_str.str();

	std::stringstream key_str;
	key_str << "Aggregate: ";
	if (!method_typename.empty())
		key_str << method_typename + ", ";
	key_str << aggregate_functions_typenames;
	std::string key = key_str.str();

	auto get_code = [method_typename, method_typename_two_level, aggregate_functions_typenames]
	{
		/// Короткий кусок кода, представляющий собой явное инстанцирование шаблона.
		std::stringstream code;
		code <<		/// Нет явного включения заголовочного файла. Он подключается с помощью опции компилятора -include.
			"namespace DB\n"
			"{\n"
			"\n";

		/// Может быть до двух инстанцирований шаблона - для обычного и two_level вариантов.
		auto append_code_for_specialization =
			[&code, &aggregate_functions_typenames] (const std::string & method_typename, const std::string & suffix)
		{
			code <<
				"template void Aggregator::executeSpecialized<\n"
					"\t" << method_typename << ", TypeList<" << aggregate_functions_typenames << ">>(\n"
					"\t" << method_typename << " &, Arena *, size_t, ConstColumnPlainPtrs &,\n"
					"\tAggregateColumns &, const Sizes &, StringRefs &, bool, AggregateDataPtr) const;\n"
				"\n"
				"static void wrapper" << suffix << "(\n"
					"\tconst Aggregator & aggregator,\n"
					"\t" << method_typename << " & method,\n"
					"\tArena * arena,\n"
					"\tsize_t rows,\n"
					"\tConstColumnPlainPtrs & key_columns,\n"
					"\tAggregator::AggregateColumns & aggregate_columns,\n"
					"\tconst Sizes & key_sizes,\n"
					"\tStringRefs & keys,\n"
					"\tbool no_more_keys,\n"
					"\tAggregateDataPtr overflow_row)\n"
				"{\n"
					"\taggregator.executeSpecialized<\n"
						"\t\t" << method_typename << ", TypeList<" << aggregate_functions_typenames << ">>(\n"
						"\t\tmethod, arena, rows, key_columns, aggregate_columns, key_sizes, keys, no_more_keys, overflow_row);\n"
				"}\n"
				"\n"
				"void * getPtr" << suffix << "() __attribute__((__visibility__(\"default\")));\n"
				"void * getPtr" << suffix << "()\n"	/// Без этой обёртки непонятно, как достать нужный символ из скомпилированной библиотеки.
				"{\n"
					"\treturn reinterpret_cast<void *>(&wrapper" << suffix << ");\n"
				"}\n";
		};

		if (!method_typename.empty())
			append_code_for_specialization(method_typename, "");
		else
		{
			/// Для метода without_key.
			code <<
				"template void Aggregator::executeSpecializedWithoutKey<\n"
					"\t" << "TypeList<" << aggregate_functions_typenames << ">>(\n"
					"\tAggregatedDataWithoutKey &, size_t, AggregateColumns &) const;\n"
				"\n"
				"static void wrapper(\n"
					"\tconst Aggregator & aggregator,\n"
					"\tAggregatedDataWithoutKey & method,\n"
					"\tsize_t rows,\n"
					"\tAggregator::AggregateColumns & aggregate_columns)\n"
				"{\n"
					"\taggregator.executeSpecializedWithoutKey<\n"
						"\t\tTypeList<" << aggregate_functions_typenames << ">>(\n"
						"\t\tmethod, rows, aggregate_columns);\n"
				"}\n"
				"\n"
				"void * getPtr() __attribute__((__visibility__(\"default\")));\n"
				"void * getPtr()\n"
				"{\n"
					"\treturn reinterpret_cast<void *>(&wrapper);\n"
				"}\n";
		}

		if (!method_typename_two_level.empty())
			append_code_for_specialization(method_typename_two_level, "TwoLevel");
		else
		{
			/// Заглушка.
			code <<
				"void * getPtrTwoLevel() __attribute__((__visibility__(\"default\")));\n"
				"void * getPtrTwoLevel()\n"
				"{\n"
					"\treturn nullptr;\n"
				"}\n";
		}

		code <<
			"}\n";

		return code.str();
	};

	auto compiled_data_owned_by_callback = compiled_data;
	auto on_ready = [compiled_data_owned_by_callback] (SharedLibraryPtr & lib)
	{
		if (compiled_data_owned_by_callback.unique())	/// Aggregator уже уничтожен.
			return;

		compiled_data_owned_by_callback->compiled_aggregator = lib;
		compiled_data_owned_by_callback->compiled_method_ptr = lib->get<void * (*) ()>("_ZN2DB6getPtrEv")();
		compiled_data_owned_by_callback->compiled_two_level_method_ptr = lib->get<void * (*) ()>("_ZN2DB14getPtrTwoLevelEv")();
	};

	/** Если библиотека уже была скомпилирована, то возвращается ненулевой SharedLibraryPtr.
	  * Если библиотека не была скомпилирована, то увеличивается счётчик, и возвращается nullptr.
	  * Если счётчик достигнул значения min_count_to_compile, то асинхронно (в отдельном потоке) запускается компиляция,
	  *  по окончании которой вызывается колбэк on_ready.
	  */
	SharedLibraryPtr lib = compiler->getOrCount(key, min_count_to_compile,
		"-include /usr/share/clickhouse/headers/dbms/include/DB/Interpreters/SpecializedAggregator.h",
		get_code, on_ready);

	/// Если результат уже готов.
	if (lib)
		on_ready(lib);
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes)
{
	/** Возвращает обычные (не two-level) методы, так как обработка начинается с них.
	  * Затем, в процессе работы, данные могут быть переконвертированы в two-level структуру, если их становится много.
	  */

	bool all_fixed = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isFixed())
		{
			all_fixed = false;
			break;
		}
		key_sizes[j] = key_columns[j]->sizeOfField();
		keys_bytes += key_sizes[j];
	}

	/// Если ключей нет
	if (keys_size == 0)
		return AggregatedDataVariants::Type::without_key;

	/// Если есть один числовой ключ, который помещается в 64 бита
	if (keys_size == 1 && key_columns[0]->isNumeric())
	{
		size_t size_of_field = key_columns[0]->sizeOfField();
		if (size_of_field == 1)
			return AggregatedDataVariants::Type::key8;
		if (size_of_field == 2)
			return AggregatedDataVariants::Type::key16;
		if (size_of_field == 4)
			return AggregatedDataVariants::Type::key32;
		if (size_of_field == 8)
			return AggregatedDataVariants::Type::key64;
		throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8.", ErrorCodes::LOGICAL_ERROR);
	}

	/// Если ключи помещаются в N бит, будем использовать хэш-таблицу по упакованным в N-бит ключам
	if (all_fixed && keys_bytes <= 16)
		return AggregatedDataVariants::Type::keys128;
	if (all_fixed && keys_bytes <= 32)
		return AggregatedDataVariants::Type::keys256;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1 && typeid_cast<const ColumnString *>(key_columns[0]))
		return AggregatedDataVariants::Type::key_string;

	if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
		return AggregatedDataVariants::Type::key_fixed_string;

	/// Иначе будем агрегировать по конкатенации ключей.
	return AggregatedDataVariants::Type::concat;

	/// NOTE AggregatedDataVariants::Type::hashed не используется.
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
	for (size_t j = 0; j < aggregates_size; ++j)
	{
		try
		{
			/** Может возникнуть исключение при нехватке памяти.
			  * Для того, чтобы потом всё правильно уничтожилось, "откатываем" часть созданных состояний.
			  * Код не очень удобный.
			  */
			aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
		}
		catch (...)
		{
			for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
				aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

			throw;
		}
	}
}


/** Интересно - если убрать noinline, то gcc зачем-то инлайнит эту функцию, и производительность уменьшается (~10%).
  * (Возможно из-за того, что после инлайна этой функции, перестают инлайниться более внутренние функции.)
  * Инлайнить не имеет смысла, так как внутренний цикл находится целиком внутри этой функции.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
	Method & method,
	Arena * aggregates_pool,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumns & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys,
	bool no_more_keys,
	AggregateDataPtr overflow_row) const
{
	typename Method::State state;
	state.init(key_columns);

	if (!no_more_keys)
		executeImplCase<false>(method, state, aggregates_pool, rows, key_columns, aggregate_columns, key_sizes, keys, overflow_row);
	else
		executeImplCase<true>(method, state, aggregates_pool, rows, key_columns, aggregate_columns, key_sizes, keys, overflow_row);
}

#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

template <bool no_more_keys, typename Method>
void NO_INLINE Aggregator::executeImplCase(
	Method & method,
	typename Method::State & state,
	Arena * aggregates_pool,
	size_t rows,
	ConstColumnPlainPtrs & key_columns,
	AggregateColumns & aggregate_columns,
	const Sizes & key_sizes,
	StringRefs & keys,
	AggregateDataPtr overflow_row) const
{
	/// NOTE При редактировании этого кода, обратите также внимание на SpecializedAggregator.h.

	/// Для всех строчек.
	typename Method::iterator it;
	typename Method::Key prev_key;
	for (size_t i = 0; i < rows; ++i)
	{
		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?
		bool overflow = false;	/// Новый ключ не поместился в хэш-таблицу из-за no_more_keys.

		/// Получаем ключ для вставки в хэш-таблицу.
		typename Method::Key key = state.getKey(key_columns, keys_size, i, key_sizes, keys, *aggregates_pool);

		if (!no_more_keys)	/// Вставляем.
		{
			/// Оптимизация для часто повторяющихся ключей.
			if (!Method::no_consecutive_keys_optimization)
			{
				if (i != 0 && key == prev_key)
				{
					/// Добавляем значения в агрегатные функции.
					AggregateDataPtr value = Method::getAggregateData(it->second);
					for (size_t j = 0; j < aggregates_size; ++j)	/// NOTE: Заменить индекс на два указателя?
						aggregate_functions[j]->add(value + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);

					method.onExistingKey(key, keys, *aggregates_pool);
					continue;
				}
				else
					prev_key = key;
			}

			method.data.emplace(key, it, inserted);
		}
		else
		{
			/// Будем добавлять только если ключ уже есть.
			inserted = false;
			it = method.data.find(key);
			if (method.data.end() == it)
				overflow = true;
		}

		/// Если ключ не поместился, и данные не надо агрегировать в отдельную строку, то делать нечего.
		if (no_more_keys && overflow && !overflow_row)
		{
			method.onExistingKey(key, keys, *aggregates_pool);
			continue;
		}

		/// Если вставили новый ключ - инициализируем состояния агрегатных функций, и возможно, что-нибудь связанное с ключом.
		if (inserted)
		{
			AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);

			/// exception-safety - если не удалось выделить память или создать состояния, то не будут вызываться деструкторы.
			aggregate_data = nullptr;

			method.onNewKey(*it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr place = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(place);
			aggregate_data = place;
		}
		else
			method.onExistingKey(key, keys, *aggregates_pool);

		AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

		/// Добавляем значения в агрегатные функции.
		for (size_t j = 0; j < aggregates_size; ++j)
			aggregate_functions[j]->add(value + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
	}
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

void NO_INLINE Aggregator::executeWithoutKeyImpl(
	AggregatedDataWithoutKey & res,
	size_t rows,
	AggregateColumns & aggregate_columns) const
{
	/// Оптимизация в случае единственной агрегатной функции count.
	AggregateFunctionCount * agg_count = aggregates_size == 1
		? typeid_cast<AggregateFunctionCount *>(aggregate_functions[0])
		: NULL;

	if (agg_count)
		agg_count->addDelta(res, rows);
	else
	{
		for (size_t i = 0; i < rows; ++i)
		{
			/// Добавляем значения
			for (size_t j = 0; j < aggregates_size; ++j)
				aggregate_functions[j]->add(res + offsets_of_aggregate_states[j], &aggregate_columns[j][0], i);
		}
	}
}


bool Aggregator::executeOnBlock(Block & block, AggregatedDataVariants & result,
	ConstColumnPlainPtrs & key_columns, AggregateColumns & aggregate_columns,
	Sizes & key_sizes, StringRefs & key,
	bool & no_more_keys)
{
	initialize(block);

	if (isCancelled())
		return true;

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_columns[i].resize(aggregates[i].arguments.size());

	/** Константные столбцы не поддерживаются напрямую при агрегации.
	  * Чтобы они всё-равно работали, материализуем их.
	  */
	Columns materialized_columns;

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByPosition(keys[i]).column;

		if (const IColumnConst * column_const = dynamic_cast<const IColumnConst *>(key_columns[i]))
		{
			materialized_columns.push_back(column_const->convertToFullColumn());
			key_columns[i] = materialized_columns.back().get();
		}
	}

	for (size_t i = 0; i < aggregates_size; ++i)
	{
		for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
		{
			aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

			if (const IColumnConst * column_const = dynamic_cast<const IColumnConst *>(aggregate_columns[i][j]))
			{
				materialized_columns.push_back(column_const->convertToFullColumn());
				aggregate_columns[i][j] = materialized_columns.back().get();
			}
		}
	}

	if (isCancelled())
		return true;

	size_t rows = block.rows();

	/// Каким способом выполнять агрегацию?
	if (result.empty())
	{
		result.init(chooseAggregationMethod(key_columns, key_sizes));
		result.keys_size = keys_size;
		result.key_sizes = key_sizes;
		LOG_TRACE(log, "Aggregation method: " << result.getMethodName());

		if (compiler)
			compileIfPossible(result.type);
	}

	if (isCancelled())
		return true;

	if ((overflow_row || result.type == AggregatedDataVariants::Type::without_key) && !result.without_key)
	{
		AggregateDataPtr place = result.aggregates_pool->alloc(total_size_of_aggregate_states);
		createAggregateStates(place);
		result.without_key = place;
	}

	/// Выбираем один из методов агрегации и вызываем его.

	/// Для случая, когда нет ключей (всё агегировать в одну строку).
	if (result.type == AggregatedDataVariants::Type::without_key)
	{
		/// Если есть динамически скомпилированный код.
		if (compiled_data->compiled_method_ptr)
		{
			reinterpret_cast<
				void (*)(const Aggregator &, AggregatedDataWithoutKey &, size_t, AggregateColumns &)>
					(compiled_data->compiled_method_ptr)(*this, result.without_key, rows, aggregate_columns);
		}
		else
			executeWithoutKeyImpl(result.without_key, rows, aggregate_columns);
	}
	else
	{
		/// Сюда пишутся данные, не поместившиеся в max_rows_to_group_by при group_by_overflow_mode = any.
		AggregateDataPtr overflow_row_ptr = overflow_row ? result.without_key : nullptr;

		bool is_two_level = result.isTwoLevel();

		/// Скомпилированный код, для обычной структуры.
		if (!is_two_level && compiled_data->compiled_method_ptr)
		{
		#define M(NAME, IS_TWO_LEVEL) \
			else if (result.type == AggregatedDataVariants::Type::NAME) \
				reinterpret_cast<void (*)( \
					const Aggregator &, decltype(result.NAME)::element_type &, \
					Arena *, size_t, ConstColumnPlainPtrs &, AggregateColumns &, \
					const Sizes &, StringRefs &, bool, AggregateDataPtr)>(compiled_data->compiled_method_ptr) \
				(*this, *result.NAME, result.aggregates_pool, rows, key_columns, aggregate_columns, \
					result.key_sizes, key, no_more_keys, overflow_row_ptr);

			if (false) {}
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M
		}
		/// Скомпилированный код, для two-level структуры.
		else if (is_two_level && compiled_data->compiled_two_level_method_ptr)
		{
		#define M(NAME) \
			else if (result.type == AggregatedDataVariants::Type::NAME) \
				reinterpret_cast<void (*)( \
					const Aggregator &, decltype(result.NAME)::element_type &, \
					Arena *, size_t, ConstColumnPlainPtrs &, AggregateColumns &, \
					const Sizes &, StringRefs &, bool, AggregateDataPtr)>(compiled_data->compiled_two_level_method_ptr) \
				(*this, *result.NAME, result.aggregates_pool, rows, key_columns, aggregate_columns, \
					result.key_sizes, key, no_more_keys, overflow_row_ptr);

			if (false) {}
			APPLY_FOR_VARIANTS_TWO_LEVEL(M)
		#undef M
		}
		/// Когда нет динамически скомпилированного кода.
		else
		{
		#define M(NAME, IS_TWO_LEVEL) \
			else if (result.type == AggregatedDataVariants::Type::NAME) \
				executeImpl(*result.NAME, result.aggregates_pool, rows, key_columns, aggregate_columns, \
					result.key_sizes, key, no_more_keys, overflow_row_ptr);

			if (false) {}
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M
		}
	}

	size_t result_size = result.sizeWithoutOverflowRow();

	if (group_by_two_level_threshold && result.isConvertibleToTwoLevel() && result_size >= group_by_two_level_threshold)
		result.convertToTwoLevel();

	/// Проверка ограничений.
	if (!no_more_keys && max_rows_to_group_by && result_size > max_rows_to_group_by)
	{
		if (group_by_overflow_mode == OverflowMode::THROW)
			throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
				+ " rows, maximum: " + toString(max_rows_to_group_by),
				ErrorCodes::TOO_MUCH_ROWS);
		else if (group_by_overflow_mode == OverflowMode::BREAK)
			return false;
		else if (group_by_overflow_mode == OverflowMode::ANY)
			no_more_keys = true;
		else
			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


/** Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	if (isCancelled())
		return;

	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);
	AggregateColumns aggregate_columns(aggregates_size);
	Sizes key_sizes;

	/** Используется, если есть ограничение на максимальное количество строк при агрегации,
	  *  и если group_by_overflow_mode == ANY.
	  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
	  *  ключам, которые уже успели попасть в набор.
	  */
	bool no_more_keys = false;

	LOG_TRACE(log, "Aggregating");

	Stopwatch watch;

	size_t src_rows = 0;
	size_t src_bytes = 0;

	/// Читаем все данные
	while (Block block = stream->read())
	{
		if (isCancelled())
			return;

		src_rows += block.rows();
		src_bytes += block.bytes();

		if (!executeOnBlock(block, result,
			key_columns, aggregate_columns, key_sizes, key,
			no_more_keys))
			break;
	}

	double elapsed_seconds = watch.elapsedSeconds();
	size_t rows = result.size();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
}


template <typename Method, typename Table>
void Aggregator::convertToBlockImpl(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes,
	bool final) const
{
	if (final)
		convertToBlockImplFinal(method, data, key_columns, final_aggregate_columns, key_sizes);
	else
		convertToBlockImplNotFinal(method, data, key_columns, aggregate_columns, key_sizes);
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplFinal(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes) const
{
	for (typename Table::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		method.insertKeyIntoColumns(*it, key_columns, keys_size, key_sizes);

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->insertResultInto(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[i],
				*final_aggregate_columns[i]);
	}
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	const Sizes & key_sizes) const
{
	size_t j = 0;
	for (typename Table::const_iterator it = data.begin(); it != data.end(); ++it, ++j)
	{
		method.insertKeyIntoColumns(*it, key_columns, keys_size, key_sizes);

		for (size_t i = 0; i < aggregates_size; ++i)
			(*aggregate_columns[i])[j] = Method::getAggregateData(it->second) + offsets_of_aggregate_states[i];
	}
}


template <typename Filler>
Block Aggregator::prepareBlockAndFill(
	AggregatedDataVariants & data_variants,
	bool final,
	size_t rows,
 	Filler && filler) const
{
	Block res = sample.cloneEmpty();

	ColumnPlainPtrs key_columns(keys_size);
	AggregateColumnsData aggregate_columns(aggregates_size);
	ColumnPlainPtrs final_aggregate_columns(aggregates_size);

	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = res.getByPosition(i).column;
		key_columns[i]->reserve(rows);
	}

	try
	{
		for (size_t i = 0; i < aggregates_size; ++i)
		{
			if (!final)
			{
				/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
				ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*res.getByPosition(i + keys_size).column);

				for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
					column_aggregate_func.addArena(data_variants.aggregates_pools[j]);

				aggregate_columns[i] = &column_aggregate_func.getData();
				aggregate_columns[i]->resize(rows);
			}
			else
			{
				ColumnWithTypeAndName & column = res.getByPosition(i + keys_size);
				column.type = aggregate_functions[i]->getReturnType();
				column.column = column.type->createColumn();
				column.column->reserve(rows);

				if (aggregate_functions[i]->isState())
				{
					/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
					ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*column.column);

					for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
						column_aggregate_func.addArena(data_variants.aggregates_pools[j]);
				}

				final_aggregate_columns[i] = column.column;
			}
		}

		filler(key_columns, aggregate_columns, final_aggregate_columns, data_variants.key_sizes, final);

		/// Изменяем размер столбцов-констант в блоке.
		size_t columns = res.columns();
		for (size_t i = 0; i < columns; ++i)
			if (res.getByPosition(i).column->isConst())
				res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);
	}
	catch (...)
	{
		/** Работа с состояниями агрегатных функций недостаточно exception-safe.
		  * Если часть столбцов aggregate_columns была resize-на, но значения не были вставлены,
		  *  то эти столбцы будут в некорректном состоянии
		  *  (ColumnAggregateFunction попытаются в деструкторе вызвать деструкторы у элементов, которых нет),
		  *  а также деструкторы будут вызываться у AggregatedDataVariants.
		  * Поэтому, вручную "откатываем" их.
		  */
		for (size_t i = 0; i < aggregates_size; ++i)
			if (aggregate_columns[i])
				aggregate_columns[i]->clear();

		throw;
	}

	return res;
}


BlocksList Aggregator::prepareBlocksAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final) const
{
	size_t rows = 1;

	auto filler = [&data_variants, this](
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes,
		bool final)
	{
		if (data_variants.type == AggregatedDataVariants::Type::without_key || overflow_row)
		{
			AggregatedDataWithoutKey & data = data_variants.without_key;

			for (size_t i = 0; i < aggregates_size; ++i)
				if (!final)
					(*aggregate_columns[i])[0] = data + offsets_of_aggregate_states[i];
				else
					aggregate_functions[i]->insertResultInto(data + offsets_of_aggregate_states[i], *final_aggregate_columns[i]);

			if (overflow_row)
				for (size_t i = 0; i < keys_size; ++i)
					key_columns[i]->insertDefault();
		}
	};

	Block block = prepareBlockAndFill(data_variants, final, rows, filler);
	if (overflow_row)
		block.info.is_overflows = true;

	BlocksList blocks;
	blocks.emplace_back(std::move(block));
	return blocks;
}

BlocksList Aggregator::prepareBlocksAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const
{
	size_t rows = data_variants.sizeWithoutOverflowRow();

	auto filler = [&data_variants, this](
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes,
		bool final)
	{
	#define M(NAME, IS_TWO_LEVEL) \
		else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
			convertToBlockImpl(*data_variants.NAME, data_variants.NAME->data, \
				key_columns, aggregate_columns, final_aggregate_columns, data_variants.key_sizes, final);

		if (false) {}
		APPLY_FOR_AGGREGATED_VARIANTS(M)
	#undef M
		else
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	};

	BlocksList blocks;
	blocks.emplace_back(prepareBlockAndFill(data_variants, final, rows, filler));
	return blocks;
}


BlocksList Aggregator::prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, boost::threadpool::pool * thread_pool) const
{
#define M(NAME) \
	else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
		return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, final, thread_pool);

	if (false) {}
	APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


template <typename Method>
BlocksList Aggregator::prepareBlocksAndFillTwoLevelImpl(
	AggregatedDataVariants & data_variants,
	Method & method,
	bool final,
	boost::threadpool::pool * thread_pool) const
{
	auto filler = [&method, this](
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes,
		bool final,
		size_t bucket)
	{
		convertToBlockImpl(method, method.data.impls[bucket],
			key_columns, aggregate_columns, final_aggregate_columns, key_sizes, final);
	};

	auto converter = [&](size_t bucket, MemoryTracker * memory_tracker)
	{
		current_memory_tracker = memory_tracker;

		Block block = prepareBlockAndFill(data_variants, final, method.data.impls[bucket].size(),
			[bucket, &filler] (
				ColumnPlainPtrs & key_columns,
				AggregateColumnsData & aggregate_columns,
				ColumnPlainPtrs & final_aggregate_columns,
				const Sizes & key_sizes,
				bool final)
			{
				filler(key_columns, aggregate_columns, final_aggregate_columns, key_sizes, final, bucket);
			});

		block.info.bucket_num = bucket;
		return block;
	};

	/// packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.

	std::vector<std::packaged_task<Block()>> tasks(Method::Data::NUM_BUCKETS);

	try
	{
		for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
		{
			if (method.data.impls[bucket].empty())
				continue;

			tasks[bucket] = std::packaged_task<Block()>(std::bind(converter, bucket, current_memory_tracker));

			if (thread_pool)
				thread_pool->schedule([bucket, &tasks] { tasks[bucket](); });
			else
				tasks[bucket]();
		}
	}
	catch (...)
	{
		/// Если этого не делать, то в случае исключения, tasks уничтожится раньше завершения потоков, и будет плохо.
		if (thread_pool)
			thread_pool->wait();

		throw;
	}

	if (thread_pool)
		thread_pool->wait();

	BlocksList blocks;

	/** Если был хотя бы один эксепшен, то следует "откатить" владение состояниями агрегатных функций в ColumnAggregateFunction-ах
	  *  - то есть, очистить их (см. комментарий в функции prepareBlockAndFill.)
	  */
	std::exception_ptr first_exception;
	for (auto & task : tasks)
	{
		if (!task.valid())
			continue;

		try
		{
			blocks.emplace_back(task.get_future().get());
		}
		catch (...)
		{
			if (!first_exception)
				first_exception = std::current_exception();
		}
	}

	if (first_exception)
	{
		for (auto & block : blocks)
		{
			for (size_t column_num = keys_size; column_num < keys_size + aggregates_size; ++column_num)
			{
				IColumn & col = *block.getByPosition(column_num).column;
				if (ColumnAggregateFunction * col_aggregate = typeid_cast<ColumnAggregateFunction *>(&col))
					col_aggregate->getData().clear();
			}
		}
		std::rethrow_exception(first_exception);
	}

	return blocks;
}


BlocksList Aggregator::convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads)
{
	if (isCancelled())
		return BlocksList();

	LOG_TRACE(log, "Converting aggregated data to blocks");

	Stopwatch watch;

	BlocksList blocks;

	/// В какой структуре данных агрегированы данные?
	if (data_variants.empty())
		return blocks;

	std::unique_ptr<boost::threadpool::pool> thread_pool;
	if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000	/// TODO Сделать настраиваемый порог.
		&& data_variants.isTwoLevel())						/// TODO Использовать общий тред-пул с функцией merge.
		thread_pool.reset(new boost::threadpool::pool(max_threads));

	try
	{
		if (isCancelled())
			return BlocksList();

		if (data_variants.type == AggregatedDataVariants::Type::without_key || overflow_row)
			blocks.splice(blocks.end(), prepareBlocksAndFillWithoutKey(data_variants, final));

		if (isCancelled())
			return BlocksList();

		if (data_variants.type != AggregatedDataVariants::Type::without_key)
		{
			if (!data_variants.isTwoLevel())
				blocks.splice(blocks.end(), prepareBlocksAndFillSingleLevel(data_variants, final));
			else
				blocks.splice(blocks.end(), prepareBlocksAndFillTwoLevel(data_variants, final, thread_pool.get()));
		}
	}
	catch (...)
	{
		/** Если был хотя бы один эксепшен, то следует "откатить" владение состояниями агрегатных функций в ColumnAggregateFunction-ах
		  *  - то есть, очистить их (см. комментарий в функции prepareBlockAndFill.)
		  */
		for (auto & block : blocks)
		{
			for (size_t column_num = keys_size; column_num < keys_size + aggregates_size; ++column_num)
			{
				IColumn & col = *block.getByPosition(column_num).column;
				if (ColumnAggregateFunction * col_aggregate = typeid_cast<ColumnAggregateFunction *>(&col))
					col_aggregate->getData().clear();
			}
		}

		throw;
	}

	if (!final)
	{
		/// data_variants не будет уничтожать состояния агрегатных функций в деструкторе. Теперь состояниями владеют ColumnAggregateFunction.
		data_variants.aggregator = nullptr;
	}

	if (isCancelled())
		return BlocksList();

	size_t rows = 0;
	size_t bytes = 0;

	for (const auto & block : blocks)
	{
		rows += block.rowsInFirstColumn();
		bytes += block.bytes();
	}

	double elapsed_seconds = watch.elapsedSeconds();
	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Converted aggregated data to blocks. "
		<< rows << " rows, " << bytes / 1048576.0 << " MiB"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << rows / elapsed_seconds << " rows/sec., " << bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

	return blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(
	Table & table_dst,
	Table & table_src) const
{
	for (auto it = table_src.begin(); it != table_src.end(); ++it)
	{
		decltype(it) res_it;
		bool inserted;
		table_dst.emplace(it->first, res_it, inserted, it.getHash());

		if (!inserted)
		{
			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->merge(
					Method::getAggregateData(res_it->second) + offsets_of_aggregate_states[i],
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

			for (size_t i = 0; i < aggregates_size; ++i)
				aggregate_functions[i]->destroy(
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);
		}
		else
		{
			res_it->second = it->second;
		}

		Method::getAggregateData(it->second) = nullptr;
	}
}


void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		AggregatedDataWithoutKey & res_data = res->without_key;
		AggregatedDataWithoutKey & current_data = non_empty_data[i]->without_key;

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i]);

		for (size_t i = 0; i < aggregates_size; ++i)
			aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

		current_data = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		AggregatedDataVariants & current = *non_empty_data[i];

		mergeDataImpl<Method>(
			getDataVariant<Method>(*res).data,
			getDataVariant<Method>(current).data);

		/// current не будет уничтожать состояния агрегатных функций в деструкторе
		current.aggregator = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeTwoLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data,
	boost::threadpool::pool * thread_pool) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];

	/// Слияние распараллеливается по корзинам - первому уровню TwoLevelHashMap.
	auto merge_bucket = [&non_empty_data, &res, this](size_t bucket, MemoryTracker * memory_tracker)
	{
		current_memory_tracker = memory_tracker;

		/// Все результаты агрегации соединяем с первым.
		for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
		{
			AggregatedDataVariants & current = *non_empty_data[i];

			mergeDataImpl<Method>(
				getDataVariant<Method>(*res).data.impls[bucket],
				getDataVariant<Method>(current).data.impls[bucket]);

			/// current не будет уничтожать состояния агрегатных функций в деструкторе
			current.aggregator = nullptr;
		}
	};

	/// packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.

	std::vector<std::packaged_task<void()>> tasks(Method::Data::NUM_BUCKETS);

	try
	{
		for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
		{
			tasks[bucket] = std::packaged_task<void()>(std::bind(merge_bucket, bucket, current_memory_tracker));

			if (thread_pool)
				thread_pool->schedule([bucket, &tasks] { tasks[bucket](); });
			else
				tasks[bucket]();
		}
	}
	catch (...)
	{
		/// Если этого не делать, то в случае исключения, tasks уничтожится раньше завершения потоков, и будет плохо.
		if (thread_pool)
			thread_pool->wait();

		throw;
	}

	if (thread_pool)
		thread_pool->wait();

	for (auto & task : tasks)
		if (task.valid())
			task.get_future().get();
}


AggregatedDataVariantsPtr Aggregator::merge(ManyAggregatedDataVariants & data_variants, size_t max_threads)
{
	if (data_variants.empty())
 		throw Exception("Empty data passed to Aggregator::merge().", ErrorCodes::EMPTY_DATA_PASSED);

	LOG_TRACE(log, "Merging aggregated data");

	Stopwatch watch;

	ManyAggregatedDataVariants non_empty_data;
	non_empty_data.reserve(data_variants.size());
	for (auto & data : data_variants)
		if (!data->empty())
			non_empty_data.push_back(data);

	if (non_empty_data.empty())
		return data_variants[0];

	if (non_empty_data.size() == 1)
		return non_empty_data[0];

	/// Если хотя бы один из вариантов двухуровневый, то переконвертируем все варианты в двухуровневые, если есть не такие.
	/// Замечание - возможно, было бы более оптимально не конвертировать одноуровневые варианты перед мерджем, а мерджить их отдельно, в конце.

	bool has_at_least_one_two_level = false;
	for (const auto & variant : non_empty_data)
	{
		if (variant->isTwoLevel())
		{
			has_at_least_one_two_level = true;
			break;
		}
	}

	if (has_at_least_one_two_level)
		for (auto & variant : non_empty_data)
			if (!variant->isTwoLevel())
				variant->convertToTwoLevel();

	AggregatedDataVariantsPtr & res = non_empty_data[0];

	size_t rows = res->size();
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		rows += non_empty_data[i]->size();
		AggregatedDataVariants & current = *non_empty_data[i];

		if (res->type != current.type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		res->aggregates_pools.insert(res->aggregates_pools.end(), current.aggregates_pools.begin(), current.aggregates_pools.end());
	}

	/// В какой структуре данных агрегированы данные?
	if (res->type == AggregatedDataVariants::Type::without_key || overflow_row)
		mergeWithoutKeyDataImpl(non_empty_data);

	std::unique_ptr<boost::threadpool::pool> thread_pool;
	if (max_threads > 1 && rows > 100000	/// TODO Сделать настраиваемый порог.
		&& res->isTwoLevel())
		thread_pool.reset(new boost::threadpool::pool(max_threads));

	/// TODO Упростить.
	if (res->type == AggregatedDataVariants::Type::key8)
		mergeSingleLevelDataImpl<decltype(res->key8)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key16)
		mergeSingleLevelDataImpl<decltype(res->key16)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key32)
		mergeSingleLevelDataImpl<decltype(res->key32)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key64)
		mergeSingleLevelDataImpl<decltype(res->key64)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key_string)
		mergeSingleLevelDataImpl<decltype(res->key_string)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key_fixed_string)
		mergeSingleLevelDataImpl<decltype(res->key_fixed_string)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::keys128)
		mergeSingleLevelDataImpl<decltype(res->keys128)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::keys256)
		mergeSingleLevelDataImpl<decltype(res->keys256)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::hashed)
		mergeSingleLevelDataImpl<decltype(res->hashed)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::concat)
		mergeSingleLevelDataImpl<decltype(res->concat)::element_type>(non_empty_data);
	else if (res->type == AggregatedDataVariants::Type::key32_two_level)
		mergeTwoLevelDataImpl<decltype(res->key32_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::key64_two_level)
		mergeTwoLevelDataImpl<decltype(res->key64_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::key_string_two_level)
		mergeTwoLevelDataImpl<decltype(res->key_string_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::key_fixed_string_two_level)
		mergeTwoLevelDataImpl<decltype(res->key_fixed_string_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::keys128_two_level)
		mergeTwoLevelDataImpl<decltype(res->keys128_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::keys256_two_level)
		mergeTwoLevelDataImpl<decltype(res->keys256_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::hashed_two_level)
		mergeTwoLevelDataImpl<decltype(res->hashed_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type == AggregatedDataVariants::Type::concat_two_level)
		mergeTwoLevelDataImpl<decltype(res->concat_two_level)::element_type>(non_empty_data, thread_pool.get());
	else if (res->type != AggregatedDataVariants::Type::without_key)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	double elapsed_seconds = watch.elapsedSeconds();
	size_t res_rows = res->size();

	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Merged aggregated data. "
		<< "From " << rows << " to " << res_rows << " rows (efficiency: " << static_cast<double>(rows) / res_rows << ")"
		<< " in " << elapsed_seconds << " sec."
		<< " (" << rows / elapsed_seconds << " rows/sec.)");

	return res;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(
	Block & block,
	const Sizes & key_sizes,
	Arena * aggregates_pool,
	Method & method,
	Table & data) const
{
	ConstColumnPlainPtrs key_columns(keys_size);
	AggregateColumnsData aggregate_columns(aggregates_size);

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = block.getByPosition(i).column;

	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_columns[i] = &typeid_cast<ColumnAggregateFunction &>(*block.getByPosition(keys_size + i).column).getData();

	typename Method::State state;
	state.init(key_columns);

	/// Для всех строчек.
	StringRefs keys(keys_size);
	size_t rows = block.rowsInFirstColumn();
	for (size_t i = 0; i < rows; ++i)
	{
		typename Table::iterator it;
		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?

		/// Получаем ключ для вставки в хэш-таблицу.
		auto key = state.getKey(key_columns, keys_size, i, key_sizes, keys, *aggregates_pool);

		data.emplace(key, it, inserted);

		if (inserted)
		{
			AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
			aggregate_data = nullptr;

			method.onNewKey(*it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr place = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(place);
			aggregate_data = place;
		}
		else
			method.onExistingKey(key, keys, *aggregates_pool);

		/// Мерджим состояния агрегатных функций.
		for (size_t j = 0; j < aggregates_size; ++j)
			aggregate_functions[j]->merge(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[j],
				(*aggregate_columns[j])[i]);
	}

	/// Пораньше освобождаем память.
	block.clear();
}

void NO_INLINE Aggregator::mergeWithoutKeyStreamsImpl(
	Block & block,
	AggregatedDataVariants & result) const
{
	AggregateColumnsData aggregate_columns(aggregates_size);

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_columns[i] = &typeid_cast<ColumnAggregateFunction &>(*block.getByPosition(keys_size + i).column).getData();

	AggregatedDataWithoutKey & res = result.without_key;
	if (!res)
	{
		AggregateDataPtr place = result.aggregates_pool->alloc(total_size_of_aggregate_states);
		createAggregateStates(place);
		res = place;
	}

	/// Добавляем значения
	for (size_t i = 0; i < aggregates_size; ++i)
		aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0]);

	/// Пораньше освобождаем память.
	block.clear();
}


void Aggregator::mergeStream(BlockInputStreamPtr stream, AggregatedDataVariants & result, size_t max_threads)
{
	if (isCancelled())
		return;

	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	AggregateColumnsData aggregate_columns(aggregates_size);

	Block empty_block;
	initialize(empty_block);

	if (isCancelled())
		return;

	/** Если на удалённых серверах использовался двухуровневый метод агрегации,
	  *  то в блоках будет расположена информация о номере корзины.
	  * Тогда вычисления можно будет распараллелить по корзинам.
	  * Разложим блоки по указанным в них номерам корзин.
	  */
	using BucketToBlocks = std::map<Int32, BlocksList>;
	BucketToBlocks bucket_to_blocks;

	/// Читаем все данные. TODO memory-savvy режим, при котором в один момент времени обрабатывается только одна корзина.
	LOG_TRACE(log, "Reading blocks of partially aggregated data.");

	size_t total_input_rows = 0;
	size_t total_input_blocks = 0;
	while (Block block = stream->read())
	{
		if (isCancelled())
			return;

		total_input_rows += block.rowsInFirstColumn();
		++total_input_blocks;
		bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(block));
	}

	LOG_TRACE(log, "Read " << total_input_blocks << " blocks of partially aggregated data, total " << total_input_rows << " rows.");

	if (bucket_to_blocks.empty())
		return;

	if (!sample)
		sample = bucket_to_blocks.begin()->second.front().cloneEmpty();

	/// Каким способом выполнять агрегацию?
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = sample.getByPosition(i).column;

	Sizes key_sizes;
	AggregatedDataVariants::Type method = chooseAggregationMethod(key_columns, key_sizes);

	/** Минус единицей обозначается отсутствие информации о корзине
	  * - в случае одноуровневой агрегации, а также для блоков с "переполнившимися" значениями.
	  * Если есть хотя бы один блок с номером корзины больше нуля, значит была двухуровневая агрегация.
	  */
	auto max_bucket = bucket_to_blocks.rbegin()->first;
	size_t has_two_level = max_bucket > 0;

	if (has_two_level)
	{
	#define M(NAME) \
		if (method == AggregatedDataVariants::Type::NAME) \
			method = AggregatedDataVariants::Type::NAME ## _two_level;

		APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

	#undef M
	}

	if (isCancelled())
		return;

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	result.init(method);
	result.keys_size = keys_size;
	result.key_sizes = key_sizes;

	bool has_blocks_with_unknown_bucket = bucket_to_blocks.count(-1);

	/// Сначала параллельно мерджим для отдельных bucket-ов. Затем домердживаем данные, не распределённые по bucket-ам.
	if (has_two_level)
	{
		LOG_TRACE(log, "Merging partially aggregated two-level data.");

		auto merge_bucket = [&bucket_to_blocks, &result, &key_sizes, this](Int32 bucket, Arena * aggregates_pool, MemoryTracker * memory_tracker)
		{
			current_memory_tracker = memory_tracker;

			for (Block & block : bucket_to_blocks[bucket])
			{
				if (isCancelled())
					return;

			#define M(NAME) \
				else if (result.type == AggregatedDataVariants::Type::NAME) \
					mergeStreamsImpl(block, key_sizes, aggregates_pool, *result.NAME, result.NAME->data.impls[bucket]);

				if (false) {}
					APPLY_FOR_VARIANTS_TWO_LEVEL(M)
			#undef M
				else
					throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
			}
		};

		/// packaged_task используются, чтобы исключения автоматически прокидывались в основной поток.

		std::vector<std::packaged_task<void()>> tasks(max_bucket + 1);

		std::unique_ptr<boost::threadpool::pool> thread_pool;
		if (max_threads > 1 && total_input_rows > 100000	/// TODO Сделать настраиваемый порог.
			&& has_two_level)
			thread_pool.reset(new boost::threadpool::pool(max_threads));

		for (const auto & bucket_blocks : bucket_to_blocks)
		{
			const auto bucket = bucket_blocks.first;

			if (bucket == -1)
				continue;

			result.aggregates_pools.push_back(new Arena);
			Arena * aggregates_pool = result.aggregates_pools.back().get();

			tasks[bucket] = std::packaged_task<void()>(std::bind(merge_bucket, bucket, aggregates_pool, current_memory_tracker));

			if (thread_pool)
				thread_pool->schedule([bucket, &tasks] { tasks[bucket](); });
			else
				tasks[bucket]();
		}

		if (thread_pool)
			thread_pool->wait();

		for (auto & task : tasks)
			if (task.valid())
				task.get_future().get();

		LOG_TRACE(log, "Merged partially aggregated two-level data.");
	}

	if (isCancelled())
	{
		result.invalidate();
		return;
	}

	if (has_blocks_with_unknown_bucket)
	{
		LOG_TRACE(log, "Merging partially aggregated single-level data.");

		BlocksList & blocks = bucket_to_blocks[-1];
		for (Block & block : blocks)
		{
			if (isCancelled())
			{
				result.invalidate();
				return;
			}

			if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
				mergeWithoutKeyStreamsImpl(block, result);

		#define M(NAME, IS_TWO_LEVEL) \
			else if (result.type == AggregatedDataVariants::Type::NAME) \
				mergeStreamsImpl(block, key_sizes, result.aggregates_pool, *result.NAME, result.NAME->data);

			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M
			else if (result.type != AggregatedDataVariants::Type::without_key)
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}

		LOG_TRACE(log, "Merged partially aggregated single-level data.");
	}
}


Block Aggregator::mergeBlocks(BlocksList & blocks, bool final)
{
	if (blocks.empty())
		return {};

	StringRefs key(keys_size);
	ConstColumnPlainPtrs key_columns(keys_size);

	AggregateColumnsData aggregate_columns(aggregates_size);

	initialize(blocks.front());

	/// Каким способом выполнять агрегацию?
	for (size_t i = 0; i < keys_size; ++i)
		key_columns[i] = sample.getByPosition(i).column;

	Sizes key_sizes;
	AggregatedDataVariants::Type method = chooseAggregationMethod(key_columns, key_sizes);

	/// Временные данные для агрегации.
	AggregatedDataVariants result;

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	result.init(method);
	result.keys_size = keys_size;
	result.key_sizes = key_sizes;

	LOG_TRACE(log, "Merging partially aggregated blocks.");

	for (Block & block : blocks)
	{
		if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
			mergeWithoutKeyStreamsImpl(block, result);

	#define M(NAME, IS_TWO_LEVEL) \
		else if (result.type == AggregatedDataVariants::Type::NAME) \
			mergeStreamsImpl(block, key_sizes, result.aggregates_pool, *result.NAME, result.NAME->data);

		APPLY_FOR_AGGREGATED_VARIANTS(M)
	#undef M
		else if (result.type != AggregatedDataVariants::Type::without_key)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}

	BlocksList merged_block = convertToBlocks(result, final, 1);

	if (merged_block.size() > 1)	/// TODO overflows
		throw Exception("Logical error: temporary result is not single-level", ErrorCodes::LOGICAL_ERROR);

	LOG_TRACE(log, "Merged partially aggregated blocks.");

	if (merged_block.empty())
		return {};

	return merged_block.front();
}


template <typename Method>
void NO_INLINE Aggregator::destroyImpl(
	Method & method) const
{
	for (typename Method::const_iterator it = method.data.begin(); it != method.data.end(); ++it)
	{
		char * data = Method::getAggregateData(it->second);

		/** Если исключение (обычно нехватка памяти, кидается MemoryTracker-ом) возникло
		  *  после вставки ключа в хэш-таблицу, но до создания всех состояний агрегатных функций,
		  *  то data будет равен nullptr-у.
		  */
		if (nullptr == data)
			continue;

		for (size_t i = 0; i < aggregates_size; ++i)
			if (!aggregate_functions[i]->isState())
				aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);
	}
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
	if (result.size() == 0)
		return;

	LOG_TRACE(log, "Destroying aggregate states");

	/// В какой структуре данных агрегированы данные?
	if (result.type == AggregatedDataVariants::Type::without_key || overflow_row)
	{
		AggregatedDataWithoutKey & res_data = result.without_key;

		if (nullptr != res_data)
			for (size_t i = 0; i < aggregates_size; ++i)
				if (!aggregate_functions[i]->isState())
					aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);
	}

#define M(NAME, IS_TWO_LEVEL) \
	else if (result.type == AggregatedDataVariants::Type::NAME) \
		destroyImpl(*result.NAME);

	if (false) {}
	APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
	else if (result.type != AggregatedDataVariants::Type::without_key)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


String Aggregator::getID() const
{
	std::stringstream res;

	if (keys.empty())
	{
		res << "key_names";
		for (size_t i = 0; i < key_names.size(); ++i)
			res << ", " << key_names[i];
	}
	else
	{
		res << "keys";
		for (size_t i = 0; i < keys.size(); ++i)
			res << ", " << keys[i];
	}

	res << ", aggregates";
	for (size_t i = 0; i < aggregates.size(); ++i)
		res << ", " << aggregates[i].column_name;

	return res.str();
}

void Aggregator::setCancellationHook(const CancellationHook cancellation_hook)
{
	isCancelled = cancellation_hook;
}


}
