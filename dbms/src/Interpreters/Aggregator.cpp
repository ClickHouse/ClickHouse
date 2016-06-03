#include <iomanip>
#include <thread>
#include <future>

#include <cxxabi.h>

#include <DB/Common/Stopwatch.h>
#include <DB/Common/setThreadName.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/AggregateFunctions/AggregateFunctionCount.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/NullBlockInputStream.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>

#include <DB/Interpreters/Aggregator.h>
#include <common/ClickHouseRevision.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_COMPILE_CODE;
	extern const int TOO_MUCH_ROWS;
	extern const int EMPTY_DATA_PASSED;
	extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
}


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


void Aggregator::Params::calculateColumnNumbers(const Block & block)
{
	if (keys.empty() && !key_names.empty())
		for (Names::const_iterator it = key_names.begin(); it != key_names.end(); ++it)
			keys.push_back(block.getPositionByName(*it));

	for (AggregateDescriptions::iterator it = aggregates.begin(); it != aggregates.end(); ++it)
		if (it->arguments.empty() && !it->argument_names.empty())
			for (Names::const_iterator jt = it->argument_names.begin(); jt != it->argument_names.end(); ++jt)
				it->arguments.push_back(block.getPositionByName(*jt));
}


void Aggregator::initialize(const Block & block)
{
	if (isCancelled())
		return;

	std::lock_guard<std::mutex> lock(mutex);

	if (initialized)
		return;

	initialized = true;

	if (current_memory_tracker)
		memory_usage_before_aggregation = current_memory_tracker->get();

	aggregate_functions.resize(params.aggregates_size);
	for (size_t i = 0; i < params.aggregates_size; ++i)
		aggregate_functions[i] = params.aggregates[i].function.get();

	/// Инициализируем размеры состояний и смещения для агрегатных функций.
	offsets_of_aggregate_states.resize(params.aggregates_size);
	total_size_of_aggregate_states = 0;
	all_aggregates_has_trivial_destructor = true;

	for (size_t i = 0; i < params.aggregates_size; ++i)
	{
		offsets_of_aggregate_states[i] = total_size_of_aggregate_states;
		total_size_of_aggregate_states += params.aggregates[i].function->sizeOfData();

		if (!params.aggregates[i].function->hasTrivialDestructor())
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
	params.calculateColumnNumbers(block);

	if (isCancelled())
		return;

	/// Создадим пример блока, описывающего результат
	if (!sample)
	{
		for (size_t i = 0; i < params.keys_size; ++i)
		{
			sample.insert(block.getByPosition(params.keys[i]).cloneEmpty());
			if (auto converted = sample.getByPosition(i).column->convertToFullColumnIfConst())
				sample.getByPosition(i).column = converted;
		}

		for (size_t i = 0; i < params.aggregates_size; ++i)
		{
			ColumnWithTypeAndName col;
			col.name = params.aggregates[i].column_name;

			size_t arguments_size = params.aggregates[i].arguments.size();
			DataTypes argument_types(arguments_size);
			for (size_t j = 0; j < arguments_size; ++j)
				argument_types[j] = block.getByPosition(params.aggregates[i].arguments[j]).type;

			col.type = new DataTypeAggregateFunction(params.aggregates[i].function, argument_types, params.aggregates[i].parameters);
			col.column = col.type->createColumn();

			sample.insert(col);
		}
	}
}


void Aggregator::setSampleBlock(const Block & block)
{
	std::lock_guard<std::mutex> lock(mutex);

	if (!sample)
		sample = block.cloneEmpty();
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
	for (size_t i = 0; i < params.aggregates_size; ++i)
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
	SharedLibraryPtr lib = params.compiler->getOrCount(key, params.min_count_to_compile,
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

	size_t num_array_keys = 0;
	bool has_arrays_of_non_fixed_elems = false;
	bool all_non_array_keys_are_fixed = true;

	key_sizes.resize(params.keys_size);
	for (size_t j = 0; j < params.keys_size; ++j)
	{
		if (key_columns[j]->isFixed())
		{
			key_sizes[j] = key_columns[j]->sizeOfField();
			keys_bytes += key_sizes[j];
		}
		else
		{
			all_fixed = false;

			if (const ColumnArray * arr = typeid_cast<const ColumnArray *>(key_columns[j]))
			{
				++num_array_keys;

				if (!arr->getData().isFixed())
					has_arrays_of_non_fixed_elems = true;
			}
			else
				all_non_array_keys_are_fixed = false;
		}
	}

	/// Если ключей нет
	if (params.keys_size == 0)
		return AggregatedDataVariants::Type::without_key;

	/// Если есть один числовой ключ, который помещается в 64 бита
	if (params.keys_size == 1 && key_columns[0]->isNumeric())
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
	if (params.keys_size == 1 && typeid_cast<const ColumnString *>(key_columns[0]))
		return AggregatedDataVariants::Type::key_string;

	if (params.keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
		return AggregatedDataVariants::Type::key_fixed_string;

	/** Если есть массивы.
	  * Если есть не более одного массива из элементов фиксированной длины, и остальные ключи фиксированной длины,
	  *  то всё ещё можно использовать метод concat. Иначе - serialized.
	  */
	if (num_array_keys > 1 || has_arrays_of_non_fixed_elems || (num_array_keys == 1 && !all_non_array_keys_are_fixed))
		return AggregatedDataVariants::Type::serialized;

	/// Иначе будем агрегировать по конкатенации ключей.
	return AggregatedDataVariants::Type::concat;

	/// NOTE AggregatedDataVariants::Type::hashed не используется.
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
	for (size_t j = 0; j < params.aggregates_size; ++j)
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
	AggregateFunctionInstruction * aggregate_instructions,
	const Sizes & key_sizes,
	StringRefs & keys,
	bool no_more_keys,
	AggregateDataPtr overflow_row) const
{
	typename Method::State state;
	state.init(key_columns);

	if (!no_more_keys)
		executeImplCase<false>(method, state, aggregates_pool, rows, key_columns, aggregate_instructions, key_sizes, keys, overflow_row);
	else
		executeImplCase<true>(method, state, aggregates_pool, rows, key_columns, aggregate_instructions, key_sizes, keys, overflow_row);
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
	AggregateFunctionInstruction * aggregate_instructions,
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
		typename Method::Key key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *aggregates_pool);

		if (!no_more_keys)	/// Вставляем.
		{
			/// Оптимизация для часто повторяющихся ключей.
			if (!Method::no_consecutive_keys_optimization)
			{
				if (i != 0 && key == prev_key)
				{
					/// Добавляем значения в агрегатные функции.
					AggregateDataPtr value = Method::getAggregateData(it->second);
					for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
						(*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i);

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

			method.onNewKey(*it, params.keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr place = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(place);
			aggregate_data = place;
		}
		else
			method.onExistingKey(key, keys, *aggregates_pool);

		AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

		/// Добавляем значения в агрегатные функции.
		for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
			(*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i);
	}
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

void NO_INLINE Aggregator::executeWithoutKeyImpl(
	AggregatedDataWithoutKey & res,
	size_t rows,
	AggregateFunctionInstruction * aggregate_instructions) const
{
	/// Оптимизация в случае единственной агрегатной функции count.
	AggregateFunctionCount * agg_count = params.aggregates_size == 1
		? typeid_cast<AggregateFunctionCount *>(aggregate_functions[0])
		: NULL;

	if (agg_count)
		agg_count->addDelta(res, rows);
	else
	{
		for (size_t i = 0; i < rows; ++i)
		{
			/// Добавляем значения
			for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
				(*inst->func)(inst->that, res + inst->state_offset, inst->arguments, i);
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

	for (size_t i = 0; i < params.aggregates_size; ++i)
		aggregate_columns[i].resize(params.aggregates[i].arguments.size());

	/** Константные столбцы не поддерживаются напрямую при агрегации.
	  * Чтобы они всё-равно работали, материализуем их.
	  */
	Columns materialized_columns;

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < params.keys_size; ++i)
	{
		key_columns[i] = block.getByPosition(params.keys[i]).column;

		if (auto converted = key_columns[i]->convertToFullColumnIfConst())
		{
			materialized_columns.push_back(converted);
			key_columns[i] = materialized_columns.back().get();
		}
	}

	AggregateFunctionInstructions aggregate_functions_instructions(params.aggregates_size + 1);
	aggregate_functions_instructions[params.aggregates_size].that = nullptr;

	for (size_t i = 0; i < params.aggregates_size; ++i)
	{
		for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
		{
			aggregate_columns[i][j] = block.getByPosition(params.aggregates[i].arguments[j]).column;

			if (auto converted = aggregate_columns[i][j]->convertToFullColumnIfConst())
			{
				materialized_columns.push_back(converted);
				aggregate_columns[i][j] = materialized_columns.back().get();
			}
		}

		aggregate_functions_instructions[i].that = aggregate_functions[i];
		aggregate_functions_instructions[i].func = aggregate_functions[i]->getAddressOfAddFunction();
		aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];
		aggregate_functions_instructions[i].arguments = &aggregate_columns[i][0];
	}

	if (isCancelled())
		return true;

	size_t rows = block.rows();

	/// Каким способом выполнять агрегацию?
	if (result.empty())
	{
		result.init(chooseAggregationMethod(key_columns, key_sizes));
		result.keys_size = params.keys_size;
		result.key_sizes = key_sizes;
		LOG_TRACE(log, "Aggregation method: " << result.getMethodName());

		if (params.compiler)
			compileIfPossible(result.type);
	}

	if (isCancelled())
		return true;

	if ((params.overflow_row || result.type == AggregatedDataVariants::Type::without_key) && !result.without_key)
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
			executeWithoutKeyImpl(result.without_key, rows, &aggregate_functions_instructions[0]);
	}
	else
	{
		/// Сюда пишутся данные, не поместившиеся в max_rows_to_group_by при group_by_overflow_mode = any.
		AggregateDataPtr overflow_row_ptr = params.overflow_row ? result.without_key : nullptr;

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
				executeImpl(*result.NAME, result.aggregates_pool, rows, key_columns, &aggregate_functions_instructions[0], \
					result.key_sizes, key, no_more_keys, overflow_row_ptr);

			if (false) {}
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M
		}
	}

	size_t result_size = result.sizeWithoutOverflowRow();
	Int64 current_memory_usage = 0;
	if (current_memory_tracker)
		current_memory_usage = current_memory_tracker->get();

	auto result_size_bytes = current_memory_usage - memory_usage_before_aggregation;	/// Здесь учитываются все результаты в сумме, из разных потоков.

	bool worth_convert_to_two_level
		= (params.group_by_two_level_threshold && result_size >= params.group_by_two_level_threshold)
		|| (params.group_by_two_level_threshold_bytes && result_size_bytes >= static_cast<Int64>(params.group_by_two_level_threshold_bytes));

	/** Преобразование в двухуровневую структуру данных.
	  * Она позволяет делать, в последующем, эффективный мердж - либо экономный по памяти, либо распараллеленный.
	  */
	if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
		result.convertToTwoLevel();

	/// Проверка ограничений.
	if (!checkLimits(result_size, no_more_keys))
		return false;

	/** Сброс данных на диск, если потребляется слишком много оперативки.
	  * Данные можно сбросить на диск только если используется двухуровневая структура агрегации.
	  */
	if (params.max_bytes_before_external_group_by
		&& result.isTwoLevel()
		&& current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by)
		&& worth_convert_to_two_level)
	{
		writeToTemporaryFile(result, result_size);
	}

	return true;
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants, size_t rows)
{
	Stopwatch watch;

	auto file = std::make_unique<Poco::TemporaryFile>(params.tmp_path);
	const std::string & path = file->path();
	WriteBufferFromFile file_buf(path);
	CompressedWriteBuffer compressed_buf(file_buf);
	NativeBlockOutputStream block_out(compressed_buf, ClickHouseRevision::get());

	LOG_DEBUG(log, "Writing part of aggregation data into temporary file " << path << ".");
	ProfileEvents::increment(ProfileEvents::ExternalAggregationWritePart);

	/// Сбрасываем только двухуровневые данные.

#define M(NAME) \
	else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
		writeToTemporaryFileImpl(data_variants, *data_variants.NAME, block_out, path);

	if (false) {}
	APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	/// NOTE Вместо освобождения памяти и создания новых хэш-таблиц и арены, можно переиспользовать старые.
	data_variants.init(data_variants.type);
	data_variants.aggregates_pools = Arenas(1, new Arena);
	data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();

	block_out.flush();
	compressed_buf.next();
	file_buf.next();

	double elapsed_seconds = watch.elapsedSeconds();
	double compressed_bytes = file_buf.count();
	double uncompressed_bytes = compressed_buf.count();

	{
		std::lock_guard<std::mutex> lock(temporary_files.mutex);
		temporary_files.files.emplace_back(std::move(file));
		temporary_files.sum_size_uncompressed += uncompressed_bytes;
		temporary_files.sum_size_compressed += compressed_bytes;
	}

	ProfileEvents::increment(ProfileEvents::ExternalAggregationCompressedBytes, compressed_bytes);
	ProfileEvents::increment(ProfileEvents::ExternalAggregationUncompressedBytes, uncompressed_bytes);

	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Written part in " << elapsed_seconds << " sec., "
		<< rows << " rows, "
		<< (uncompressed_bytes / 1048576.0) << " MiB uncompressed, "
		<< (compressed_bytes / 1048576.0) << " MiB compressed, "
		<< (uncompressed_bytes / rows) << " uncompressed bytes per row, "
		<< (compressed_bytes / rows) << " compressed bytes per row, "
		<< "compression rate: " << (uncompressed_bytes / compressed_bytes)
		<< " (" << (rows / elapsed_seconds) << " rows/sec., "
		<< (uncompressed_bytes / elapsed_seconds / 1048576.0) << " MiB/sec. uncompressed, "
		<< (compressed_bytes / elapsed_seconds / 1048576.0) << " MiB/sec. compressed)");
}


template <typename Method>
Block Aggregator::convertOneBucketToBlock(
	AggregatedDataVariants & data_variants,
	Method & method,
	bool final,
	size_t bucket) const
{
	Block block = prepareBlockAndFill(data_variants, final, method.data.impls[bucket].size(),
		[bucket, &method, this] (
			ColumnPlainPtrs & key_columns,
			AggregateColumnsData & aggregate_columns,
			ColumnPlainPtrs & final_aggregate_columns,
			const Sizes & key_sizes,
			bool final)
		{
			convertToBlockImpl(method, method.data.impls[bucket],
				key_columns, aggregate_columns, final_aggregate_columns, key_sizes, final);
		});

	block.info.bucket_num = bucket;
	return block;
}


template <typename Method>
void Aggregator::writeToTemporaryFileImpl(
	AggregatedDataVariants & data_variants,
	Method & method,
	IBlockOutputStream & out,
	const String & path)
{
	size_t max_temporary_block_size_rows = 0;
	size_t max_temporary_block_size_bytes = 0;

	for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
	{
		Block block = convertOneBucketToBlock(data_variants, method, false, bucket);
		out.write(block);

		size_t block_size_rows = block.rowsInFirstColumn();
		size_t block_size_bytes = block.bytes();

		if (block_size_rows > max_temporary_block_size_rows)
			max_temporary_block_size_rows = block.rowsInFirstColumn();
		if (block_size_bytes > max_temporary_block_size_bytes)
			max_temporary_block_size_bytes = block_size_bytes;
	}

	/// data_variants не будет уничтожать состояния агрегатных функций в деструкторе. Теперь состояниями владеют ColumnAggregateFunction.
	data_variants.aggregator = nullptr;

	LOG_TRACE(log, std::fixed << std::setprecision(3)
		<< "Max size of temporary block: " << max_temporary_block_size_rows << " rows, "
		<< (max_temporary_block_size_bytes / 1048576.0) << " MiB.");
}


bool Aggregator::checkLimits(size_t result_size, bool & no_more_keys) const
{
	if (!no_more_keys && params.max_rows_to_group_by && result_size > params.max_rows_to_group_by)
	{
		if (params.group_by_overflow_mode == OverflowMode::THROW)
			throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
				+ " rows, maximum: " + toString(params.max_rows_to_group_by),
				ErrorCodes::TOO_MUCH_ROWS);
		else if (params.group_by_overflow_mode == OverflowMode::BREAK)
			return false;
		else if (params.group_by_overflow_mode == OverflowMode::ANY)
			no_more_keys = true;
		else
			throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


void Aggregator::execute(BlockInputStreamPtr stream, AggregatedDataVariants & result)
{
	if (isCancelled())
		return;

	StringRefs key(params.keys_size);
	ConstColumnPlainPtrs key_columns(params.keys_size);
	AggregateColumns aggregate_columns(params.aggregates_size);
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
	if (data.empty())
		return;

	if (final)
		convertToBlockImplFinal(method, data, key_columns, final_aggregate_columns, key_sizes);
	else
		convertToBlockImplNotFinal(method, data, key_columns, aggregate_columns, key_sizes);

	/// Для того, чтобы пораньше освободить память.
	data.clearAndShrink();
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplFinal(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	ColumnPlainPtrs & final_aggregate_columns,
	const Sizes & key_sizes) const
{
	for (const auto & value : data)
	{
		method.insertKeyIntoColumns(value, key_columns, params.keys_size, key_sizes);

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->insertResultInto(
				Method::getAggregateData(value.second) + offsets_of_aggregate_states[i],
				*final_aggregate_columns[i]);
	}

	destroyImpl(method, data);		/// NOTE Можно сделать лучше.
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
	Method & method,
	Table & data,
	ColumnPlainPtrs & key_columns,
	AggregateColumnsData & aggregate_columns,
	const Sizes & key_sizes) const
{
	for (auto & value : data)
	{
		method.insertKeyIntoColumns(value, key_columns, params.keys_size, key_sizes);

		/// reserved, поэтому push_back не кидает исключений
		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_columns[i]->push_back(Method::getAggregateData(value.second) + offsets_of_aggregate_states[i]);

		Method::getAggregateData(value.second) = nullptr;
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

	ColumnPlainPtrs key_columns(params.keys_size);
	AggregateColumnsData aggregate_columns(params.aggregates_size);
	ColumnPlainPtrs final_aggregate_columns(params.aggregates_size);

	for (size_t i = 0; i < params.keys_size; ++i)
	{
		key_columns[i] = res.getByPosition(i).column;
		key_columns[i]->reserve(rows);
	}

	for (size_t i = 0; i < params.aggregates_size; ++i)
	{
		if (!final)
		{
			/// Столбец ColumnAggregateFunction захватывает разделяемое владение ареной с состояниями агрегатных функций.
			ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(
				*res.getByPosition(i + params.keys_size).column);

			for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
				column_aggregate_func.addArena(data_variants.aggregates_pools[j]);

			aggregate_columns[i] = &column_aggregate_func.getData();
			aggregate_columns[i]->reserve(rows);
		}
		else
		{
			ColumnWithTypeAndName & column = res.getByPosition(i + params.keys_size);
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

	return res;
}


BlocksList Aggregator::prepareBlocksAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const
{
	size_t rows = 1;

	auto filler = [&data_variants, this](
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes,
		bool final)
	{
		if (data_variants.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
		{
			AggregatedDataWithoutKey & data = data_variants.without_key;

			for (size_t i = 0; i < params.aggregates_size; ++i)
			{
				if (!final)
					aggregate_columns[i]->push_back(data + offsets_of_aggregate_states[i]);
				else
					aggregate_functions[i]->insertResultInto(data + offsets_of_aggregate_states[i], *final_aggregate_columns[i]);
			}

			if (!final)
				data = nullptr;

			if (params.overflow_row)
				for (size_t i = 0; i < params.keys_size; ++i)
					key_columns[i]->insertDefault();
		}
	};

	Block block = prepareBlockAndFill(data_variants, final, rows, filler);

	if (is_overflows)
		block.info.is_overflows = true;

	if (final)
		destroyWithoutKey(data_variants);

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
	#define M(NAME) \
		else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
			convertToBlockImpl(*data_variants.NAME, data_variants.NAME->data, \
				key_columns, aggregate_columns, final_aggregate_columns, data_variants.key_sizes, final);

		if (false) {}
		APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
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
	auto converter = [&](size_t bucket, MemoryTracker * memory_tracker)
	{
		current_memory_tracker = memory_tracker;
		return convertOneBucketToBlock(data_variants, method, final, bucket);
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

	for (auto & task : tasks)
	{
		if (!task.valid())
			continue;

		blocks.emplace_back(task.get_future().get());
	}

	return blocks;
}


BlocksList Aggregator::convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const
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

	if (isCancelled())
		return BlocksList();

	if (data_variants.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
		blocks.splice(blocks.end(), prepareBlocksAndFillWithoutKey(
			data_variants, final, data_variants.type != AggregatedDataVariants::Type::without_key));

	if (isCancelled())
		return BlocksList();

	if (data_variants.type != AggregatedDataVariants::Type::without_key)
	{
		if (!data_variants.isTwoLevel())
			blocks.splice(blocks.end(), prepareBlocksAndFillSingleLevel(data_variants, final));
		else
			blocks.splice(blocks.end(), prepareBlocksAndFillTwoLevel(data_variants, final, thread_pool.get()));
	}

	if (!final)
	{
		/// data_variants не будет уничтожать состояния агрегатных функций в деструкторе.
		/// Теперь состояниями владеют ColumnAggregateFunction.
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
			for (size_t i = 0; i < params.aggregates_size; ++i)
				aggregate_functions[i]->merge(
					Method::getAggregateData(res_it->second) + offsets_of_aggregate_states[i],
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

			for (size_t i = 0; i < params.aggregates_size; ++i)
				aggregate_functions[i]->destroy(
					Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);
		}
		else
		{
			res_it->second = it->second;
		}

		Method::getAggregateData(it->second) = nullptr;
	}

	table_src.clearAndShrink();
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNoMoreKeysImpl(
	Table & table_dst,
	AggregatedDataWithoutKey & overflows,
	Table & table_src) const
{
	for (auto it = table_src.begin(); it != table_src.end(); ++it)
	{
		decltype(it) res_it = table_dst.find(it->first, it.getHash());

		AggregateDataPtr res_data = table_dst.end() == res_it
			? overflows
			: Method::getAggregateData(res_it->second);

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->merge(
				res_data + offsets_of_aggregate_states[i],
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->destroy(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

		Method::getAggregateData(it->second) = nullptr;
	}

	table_src.clearAndShrink();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataOnlyExistingKeysImpl(
	Table & table_dst,
	Table & table_src) const
{
	for (auto it = table_src.begin(); it != table_src.end(); ++it)
	{
		decltype(it) res_it = table_dst.find(it->first, it.getHash());

		if (table_dst.end() == res_it)
			continue;

		AggregateDataPtr res_data = Method::getAggregateData(res_it->second);

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->merge(
				res_data + offsets_of_aggregate_states[i],
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->destroy(
				Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

		Method::getAggregateData(it->second) = nullptr;
	}

	table_src.clearAndShrink();
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

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i]);

		for (size_t i = 0; i < params.aggregates_size; ++i)
			aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

		current_data = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(
	ManyAggregatedDataVariants & non_empty_data) const
{
	AggregatedDataVariantsPtr & res = non_empty_data[0];
	bool no_more_keys = false;

	/// Все результаты агрегации соединяем с первым.
	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		if (!checkLimits(res->sizeWithoutOverflowRow(), no_more_keys))
			break;

		AggregatedDataVariants & current = *non_empty_data[i];

		if (!no_more_keys)
			mergeDataImpl<Method>(
				getDataVariant<Method>(*res).data,
				getDataVariant<Method>(current).data);
		else if (res->without_key)
			mergeDataNoMoreKeysImpl<Method>(
				getDataVariant<Method>(*res).data,
				res->without_key,
				getDataVariant<Method>(current).data);
		else
			mergeDataOnlyExistingKeysImpl<Method>(
				getDataVariant<Method>(*res).data,
				getDataVariant<Method>(current).data);

		/// current не будет уничтожать состояния агрегатных функций в деструкторе
		current.aggregator = nullptr;
	}
}


template <typename Method>
void NO_INLINE Aggregator::mergeBucketImpl(
	ManyAggregatedDataVariants & data, Int32 bucket) const
{
	/// Все результаты агрегации соединяем с первым.
	AggregatedDataVariantsPtr & res = data[0];
	for (size_t i = 1, size = data.size(); i < size; ++i)
	{
		AggregatedDataVariants & current = *data[i];

		mergeDataImpl<Method>(
			getDataVariant<Method>(*res).data.impls[bucket],
			getDataVariant<Method>(current).data.impls[bucket]);
	}
}


/** Объединят вместе состояния агрегации, превращает их в блоки и выдаёт потоково.
  * Если состояния агрегации двухуровневые, то выдаёт блоки строго по порядку bucket_num.
  * (Это важно при распределённой обработке.)
  * При этом, может обрабатывать разные bucket-ы параллельно, используя до threads потоков.
  */
class MergingAndConvertingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/** На вход подаётся набор непустых множеств частично агрегированных данных,
	  *  которые все либо являются одноуровневыми, либо являются двухуровневыми.
	  */
	MergingAndConvertingBlockInputStream(const Aggregator & aggregator_, ManyAggregatedDataVariants & data_, bool final_, size_t threads_)
		: aggregator(aggregator_), data(data_), final(final_), threads(threads_) {}

	String getName() const override { return "MergingAndConverting"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

protected:
	Block readImpl() override
	{
		if (data.empty())
			return {};

		if (current_bucket_num >= NUM_BUCKETS)
			return {};

		AggregatedDataVariantsPtr & first = data[0];

		if (current_bucket_num == -1)
		{
			++current_bucket_num;

			if (first->type == AggregatedDataVariants::Type::without_key || aggregator.params.overflow_row)
			{
				aggregator.mergeWithoutKeyDataImpl(data);
				return aggregator.prepareBlocksAndFillWithoutKey(
					*first, final, first->type != AggregatedDataVariants::Type::without_key).front();
			}
		}

		if (!first->isTwoLevel())
		{
			if (current_bucket_num > 0)
				return {};

			if (first->type == AggregatedDataVariants::Type::without_key)
				return {};

			++current_bucket_num;

		#define M(NAME) \
			else if (first->type == AggregatedDataVariants::Type::NAME) \
				aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(data);
			if (false) {}
			APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
		#undef M
			else
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

			return aggregator.prepareBlocksAndFillSingleLevel(*first, final).front();
		}
		else
		{
			if (!parallel_merge_data)
			{
				parallel_merge_data.reset(new ParallelMergeData(threads));
				for (size_t i = 0; i < threads; ++i)
					scheduleThreadForNextBucket();
			}

			Block res;

			while (true)
			{
				std::unique_lock<std::mutex> lock(parallel_merge_data->mutex);

				if (parallel_merge_data->exception)
					std::rethrow_exception(parallel_merge_data->exception);

				auto it = parallel_merge_data->ready_blocks.find(current_bucket_num);
				if (it != parallel_merge_data->ready_blocks.end())
				{
					++current_bucket_num;
					scheduleThreadForNextBucket();

					if (it->second)
					{
						res.swap(it->second);
						break;
					}
					else if (current_bucket_num >= NUM_BUCKETS)
						break;
				}

				parallel_merge_data->condvar.wait(lock);
			}

			return res;
		}
	}

private:
	const Aggregator & aggregator;
	ManyAggregatedDataVariants data;
	bool final;
	size_t threads;

	Int32 current_bucket_num = -1;
	Int32 max_scheduled_bucket_num = -1;
	static constexpr Int32 NUM_BUCKETS = 256;

	struct ParallelMergeData
	{
		boost::threadpool::pool pool;
		std::map<Int32, Block> ready_blocks;
		std::exception_ptr exception;
		std::mutex mutex;
		std::condition_variable condvar;

		ParallelMergeData(size_t threads) : pool(threads) {}

		~ParallelMergeData()
		{
			LOG_TRACE(&Logger::get(__PRETTY_FUNCTION__), "Waiting for threads to finish");
			pool.wait();
		}
	};

	std::unique_ptr<ParallelMergeData> parallel_merge_data;

	void scheduleThreadForNextBucket()
	{
		++max_scheduled_bucket_num;
		if (max_scheduled_bucket_num >= NUM_BUCKETS)
			return;

		parallel_merge_data->pool.schedule(std::bind(&MergingAndConvertingBlockInputStream::thread, this,
			max_scheduled_bucket_num, current_memory_tracker));
	}

	void thread(Int32 bucket_num, MemoryTracker * memory_tracker)
	{
		current_memory_tracker = memory_tracker;
		setThreadName("MergingAggregtd");
		CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

		try
		{
			/// TODO Возможно, поддержать no_more_keys

			auto & merged_data = *data[0];
			auto method = merged_data.type;
			Block block;

			if (false) {}
		#define M(NAME) \
			else if (method == AggregatedDataVariants::Type::NAME) \
			{ \
				aggregator.mergeBucketImpl<decltype(merged_data.NAME)::element_type>(data, bucket_num); \
				block = aggregator.convertOneBucketToBlock(merged_data, *merged_data.NAME, final, bucket_num); \
			}

			APPLY_FOR_VARIANTS_TWO_LEVEL(M)
		#undef M

			std::lock_guard<std::mutex> lock(parallel_merge_data->mutex);
			parallel_merge_data->ready_blocks[bucket_num] = std::move(block);
		}
		catch (...)
		{
			std::lock_guard<std::mutex> lock(parallel_merge_data->mutex);
			if (!parallel_merge_data->exception)
				parallel_merge_data->exception = std::current_exception();
		}

		parallel_merge_data->condvar.notify_all();
	}
};


std::unique_ptr<IBlockInputStream> Aggregator::mergeAndConvertToBlocks(
	ManyAggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
	if (data_variants.empty())
		throw Exception("Empty data passed to Aggregator::mergeAndConvertToBlocks.", ErrorCodes::EMPTY_DATA_PASSED);

	LOG_TRACE(log, "Merging aggregated data");

	ManyAggregatedDataVariants non_empty_data;
	non_empty_data.reserve(data_variants.size());
	for (auto & data : data_variants)
		if (!data->empty())
			non_empty_data.push_back(data);

	if (non_empty_data.empty())
		return std::unique_ptr<IBlockInputStream>(new NullBlockInputStream);

	if (non_empty_data.size() > 1)
	{
		/// Отсортируем состояния по убыванию размера, чтобы мердж был более эффективным (так как все состояния мерджатся в первое).
		std::sort(non_empty_data.begin(), non_empty_data.end(),
			[](const AggregatedDataVariantsPtr & lhs, const AggregatedDataVariantsPtr & rhs)
			{
				return lhs->sizeWithoutOverflowRow() > rhs->sizeWithoutOverflowRow();
			});
	}

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

	AggregatedDataVariantsPtr & first = non_empty_data[0];

	for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
	{
		if (first->type != non_empty_data[i]->type)
			throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

		/** В первое множество данных могут быть перемещены элементы из остальных множеств.
		  * Поэтому, оно должно владеть всеми аренами всех остальных множеств.
		  */
		first->aggregates_pools.insert(first->aggregates_pools.end(),
			non_empty_data[i]->aggregates_pools.begin(), non_empty_data[i]->aggregates_pools.end());
	}

	return std::unique_ptr<IBlockInputStream>(new MergingAndConvertingBlockInputStream(*this, non_empty_data, final, max_threads));
}


template <bool no_more_keys, typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImplCase(
	Block & block,
	const Sizes & key_sizes,
	Arena * aggregates_pool,
	Method & method,
	Table & data,
	AggregateDataPtr overflow_row) const
{
	ConstColumnPlainPtrs key_columns(params.keys_size);
	AggregateColumnsData aggregate_columns(params.aggregates_size);

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < params.keys_size; ++i)
		key_columns[i] = block.getByPosition(i).column;

	for (size_t i = 0; i < params.aggregates_size; ++i)
		aggregate_columns[i] = &typeid_cast<ColumnAggregateFunction &>(*block.getByPosition(params.keys_size + i).column).getData();

	typename Method::State state;
	state.init(key_columns);

	/// Для всех строчек.
	StringRefs keys(params.keys_size);
	size_t rows = block.rowsInFirstColumn();
	for (size_t i = 0; i < rows; ++i)
	{
		typename Table::iterator it;

		bool inserted;			/// Вставили новый ключ, или такой ключ уже был?
		bool overflow = false;	/// Новый ключ не поместился в хэш-таблицу из-за no_more_keys.

		/// Получаем ключ для вставки в хэш-таблицу.
		auto key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *aggregates_pool);

		if (!no_more_keys)
		{
			data.emplace(key, it, inserted);
		}
		else
		{
			inserted = false;
			it = data.find(key);
			if (data.end() == it)
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
			aggregate_data = nullptr;

			method.onNewKey(*it, params.keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr place = aggregates_pool->alloc(total_size_of_aggregate_states);
			createAggregateStates(place);
			aggregate_data = place;
		}
		else
			method.onExistingKey(key, keys, *aggregates_pool);

		AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

		/// Мерджим состояния агрегатных функций.
		for (size_t j = 0; j < params.aggregates_size; ++j)
			aggregate_functions[j]->merge(
				value + offsets_of_aggregate_states[j],
				(*aggregate_columns[j])[i]);
	}

	/// Пораньше освобождаем память.
	block.clear();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(
	Block & block,
	const Sizes & key_sizes,
	Arena * aggregates_pool,
	Method & method,
	Table & data,
	AggregateDataPtr overflow_row,
	bool no_more_keys) const
{
	if (!no_more_keys)
		mergeStreamsImplCase<false>(block, key_sizes, aggregates_pool, method, data, overflow_row);
	else
		mergeStreamsImplCase<true>(block, key_sizes, aggregates_pool, method, data, overflow_row);
}


void NO_INLINE Aggregator::mergeWithoutKeyStreamsImpl(
	Block & block,
	AggregatedDataVariants & result) const
{
	AggregateColumnsData aggregate_columns(params.aggregates_size);

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < params.aggregates_size; ++i)
		aggregate_columns[i] = &typeid_cast<ColumnAggregateFunction &>(*block.getByPosition(params.keys_size + i).column).getData();

	AggregatedDataWithoutKey & res = result.without_key;
	if (!res)
	{
		AggregateDataPtr place = result.aggregates_pool->alloc(total_size_of_aggregate_states);
		createAggregateStates(place);
		res = place;
	}

	/// Добавляем значения
	for (size_t i = 0; i < params.aggregates_size; ++i)
		aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0]);

	/// Пораньше освобождаем память.
	block.clear();
}


void Aggregator::mergeStream(BlockInputStreamPtr stream, AggregatedDataVariants & result, size_t max_threads)
{
	if (isCancelled())
		return;

	StringRefs key(params.keys_size);
	ConstColumnPlainPtrs key_columns(params.keys_size);

	AggregateColumnsData aggregate_columns(params.aggregates_size);

	initialize({});

	if (isCancelled())
		return;

	/** Если на удалённых серверах использовался двухуровневый метод агрегации,
	  *  то в блоках будет расположена информация о номере корзины.
	  * Тогда вычисления можно будет распараллелить по корзинам.
	  * Разложим блоки по указанным в них номерам корзин.
	  */
	using BucketToBlocks = std::map<Int32, BlocksList>;
	BucketToBlocks bucket_to_blocks;

	/// Читаем все данные.
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

	setSampleBlock(bucket_to_blocks.begin()->second.front());

	/// Каким способом выполнять агрегацию?
	for (size_t i = 0; i < params.keys_size; ++i)
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
	result.keys_size = params.keys_size;
	result.key_sizes = key_sizes;

	bool has_blocks_with_unknown_bucket = bucket_to_blocks.count(-1);

	/// Сначала параллельно мерджим для отдельных bucket-ов. Затем домердживаем данные, не распределённые по bucket-ам.
	if (has_two_level)
	{
		/** В этом случае, no_more_keys не поддерживается в связи с тем, что
		  *  из разных потоков трудно обновлять общее состояние для "остальных" ключей (overflows).
		  * То есть, ключей в итоге может оказаться существенно больше, чем max_rows_to_group_by.
		  */

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
					mergeStreamsImpl(block, key_sizes, aggregates_pool, *result.NAME, result.NAME->data.impls[bucket], nullptr, false);

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

		bool no_more_keys = false;

		BlocksList & blocks = bucket_to_blocks[-1];
		for (Block & block : blocks)
		{
			if (isCancelled())
			{
				result.invalidate();
				return;
			}

			if (!checkLimits(result.sizeWithoutOverflowRow(), no_more_keys))
				break;

			if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
				mergeWithoutKeyStreamsImpl(block, result);

		#define M(NAME, IS_TWO_LEVEL) \
			else if (result.type == AggregatedDataVariants::Type::NAME) \
				mergeStreamsImpl(block, key_sizes, result.aggregates_pool, *result.NAME, result.NAME->data, result.without_key, no_more_keys);

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

	StringRefs key(params.keys_size);
	ConstColumnPlainPtrs key_columns(params.keys_size);

	AggregateColumnsData aggregate_columns(params.aggregates_size);

	initialize({});
	setSampleBlock(blocks.front());

	/// Каким способом выполнять агрегацию?
	for (size_t i = 0; i < params.keys_size; ++i)
		key_columns[i] = sample.getByPosition(i).column;

	Sizes key_sizes;
	AggregatedDataVariants::Type method = chooseAggregationMethod(key_columns, key_sizes);

	/// Временные данные для агрегации.
	AggregatedDataVariants result;

	/// result будет уничтожать состояния агрегатных функций в деструкторе
	result.aggregator = this;

	result.init(method);
	result.keys_size = params.keys_size;
	result.key_sizes = key_sizes;

	auto bucket_num = blocks.front().info.bucket_num;
	LOG_TRACE(log, "Merging partially aggregated blocks (bucket = " << bucket_num << ").");

	for (Block & block : blocks)
	{
		if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
			mergeWithoutKeyStreamsImpl(block, result);

	#define M(NAME, IS_TWO_LEVEL) \
		else if (result.type == AggregatedDataVariants::Type::NAME) \
			mergeStreamsImpl(block, key_sizes, result.aggregates_pool, *result.NAME, result.NAME->data, nullptr, false);

		APPLY_FOR_AGGREGATED_VARIANTS(M)
	#undef M
		else if (result.type != AggregatedDataVariants::Type::without_key)
			throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}

	BlocksList merged_blocks = convertToBlocks(result, final, 1);

	if (merged_blocks.size() > 1)
	{
		/** Может быть два блока. Один с is_overflows, другой - нет.
		  * Если есть непустой блок не is_overflows, то удаляем блок с is_overflows.
		  * Если есть пустой блок не is_overflows и блок с is_overflows, то удаляем пустой блок.
		  *
		  * Это делаем, потому что исходим из допущения, что в функцию передаются
		  *  либо все блоки не is_overflows, либо все блоки is_overflows.
		  */

		bool has_nonempty_nonoverflows = false;
		bool has_overflows = false;

		for (const auto & block : merged_blocks)
		{
			if (block && block.rowsInFirstColumn() && !block.info.is_overflows)
				has_nonempty_nonoverflows = true;
			else if (block.info.is_overflows)
				has_overflows = true;
		}

		if (has_nonempty_nonoverflows)
		{
			for (auto it = merged_blocks.begin(); it != merged_blocks.end(); ++it)
			{
				if (it->info.is_overflows)
				{
					merged_blocks.erase(it);
					break;
				}
			}
		}
		else if (has_overflows)
		{
			for (auto it = merged_blocks.begin(); it != merged_blocks.end(); ++it)
			{
				if (!*it || it->rowsInFirstColumn() == 0)
				{
					merged_blocks.erase(it);
					break;
				}
			}
		}

		if (merged_blocks.size() > 1)
			throw Exception("Logical error: temporary result is not single-level", ErrorCodes::LOGICAL_ERROR);
	}

	LOG_TRACE(log, "Merged partially aggregated blocks.");

	if (merged_blocks.empty())
		return {};

	auto res = std::move(merged_blocks.front());
	res.info.bucket_num = bucket_num;
	return res;
}


template <typename Method>
void NO_INLINE Aggregator::convertBlockToTwoLevelImpl(
	Method & method,
	Arena * pool,
	ConstColumnPlainPtrs & key_columns,
	const Sizes & key_sizes,
	StringRefs & keys,
	const Block & source,
	std::vector<Block> & destinations) const
{
	typename Method::State state;
	state.init(key_columns);

	size_t rows = source.rowsInFirstColumn();
	size_t columns = source.columns();

	/// Для каждого номера корзины создадим фильтр, где будут отмечены строки, относящиеся к этой корзине.
	std::vector<IColumn::Filter> filters(destinations.size());

	/// Для всех строчек.
	for (size_t i = 0; i < rows; ++i)
	{
		/// Получаем ключ. Вычисляем на его основе номер корзины.
		typename Method::Key key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *pool);

		auto hash = method.data.hash(key);
		auto bucket = method.data.getBucketFromHash(hash);

		/// Этот ключ нам больше не нужен.
		method.onExistingKey(key, keys, *pool);

		auto & filter = filters[bucket];

		if (unlikely(filter.empty()))
			filter.resize_fill(rows);

		filter[i] = 1;
	}

	ssize_t size_hint = ((source.rowsInFirstColumn() + method.data.NUM_BUCKETS - 1)
		/ method.data.NUM_BUCKETS) * 1.1;	/// Число 1.1 выбрано наугад.

	for (size_t bucket = 0, size = destinations.size(); bucket < size; ++bucket)
	{
		const auto & filter = filters[bucket];

		if (filter.empty())
			continue;

		Block & dst = destinations[bucket];
		dst.info.bucket_num = bucket;

		for (size_t j = 0; j < columns; ++j)
		{
			const ColumnWithTypeAndName & src_col = source.unsafeGetByPosition(j);
			dst.insert({src_col.column->filter(filter, size_hint), src_col.type, src_col.name});

			/** Вставленные в блок столбцы типа ColumnAggregateFunction будут владеть состояниями агрегатных функций
			  *  путём удержания SharedPtr-а на исходный столбец. См. ColumnAggregateFunction.h
			  */
		}
	}
}


std::vector<Block> Aggregator::convertBlockToTwoLevel(const Block & block)
{
	if (!block)
		return {};

	initialize({});
	setSampleBlock(block);

	AggregatedDataVariants data;

	StringRefs key(params.keys_size);
	ConstColumnPlainPtrs key_columns(params.keys_size);
	Sizes key_sizes;

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < params.keys_size; ++i)
		key_columns[i] = block.getByPosition(i).column;

	AggregatedDataVariants::Type type = chooseAggregationMethod(key_columns, key_sizes);
	data.keys_size = params.keys_size;
	data.key_sizes = key_sizes;

#define M(NAME) \
	else if (type == AggregatedDataVariants::Type::NAME) \
		type = AggregatedDataVariants::Type::NAME ## _two_level;

	if (false) {}
	APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	data.init(type);

	size_t num_buckets = 0;

#define M(NAME) \
	else if (data.type == AggregatedDataVariants::Type::NAME) \
		num_buckets = data.NAME->data.NUM_BUCKETS;

	if (false) {}
	APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	std::vector<Block> splitted_blocks(num_buckets);

#define M(NAME) \
	else if (data.type == AggregatedDataVariants::Type::NAME) \
		convertBlockToTwoLevelImpl(*data.NAME, data.aggregates_pool, \
			key_columns, data.key_sizes, key, block, splitted_blocks);

	if (false) {}
	APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
	else
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

	return splitted_blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::destroyImpl(
	Method & method,
	Table & table) const
{
	for (auto elem : table)
	{
		AggregateDataPtr & data = Method::getAggregateData(elem.second);

		/** Если исключение (обычно нехватка памяти, кидается MemoryTracker-ом) возникло
		  *  после вставки ключа в хэш-таблицу, но до создания всех состояний агрегатных функций,
		  *  то data будет равен nullptr-у.
		  */
		if (nullptr == data)
			continue;

		for (size_t i = 0; i < params.aggregates_size; ++i)
			if (!aggregate_functions[i]->isState())
				aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);

		data = nullptr;
	}
}


void Aggregator::destroyWithoutKey(AggregatedDataVariants & result) const
{
	AggregatedDataWithoutKey & res_data = result.without_key;

	if (nullptr != res_data)
	{
		for (size_t i = 0; i < params.aggregates_size; ++i)
			if (!aggregate_functions[i]->isState())
				aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);

		res_data = nullptr;
	}
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
	if (result.size() == 0)
		return;

	LOG_TRACE(log, "Destroying aggregate states");

	/// В какой структуре данных агрегированы данные?
	if (result.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
		destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL) \
	else if (result.type == AggregatedDataVariants::Type::NAME) \
		destroyImpl(*result.NAME, result.NAME->data);

	if (false) {}
	APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
	else if (result.type != AggregatedDataVariants::Type::without_key)
		throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


String Aggregator::getID() const
{
	std::stringstream res;

	if (params.keys.empty())
	{
		res << "key_names";
		for (size_t i = 0; i < params.key_names.size(); ++i)
			res << ", " << params.key_names[i];
	}
	else
	{
		res << "keys";
		for (size_t i = 0; i < params.keys.size(); ++i)
			res << ", " << params.keys[i];
	}

	res << ", aggregates";
	for (size_t i = 0; i < params.aggregates_size; ++i)
		res << ", " << params.aggregates[i].column_name;

	return res.str();
}

void Aggregator::setCancellationHook(const CancellationHook cancellation_hook)
{
	isCancelled = cancellation_hook;
}


}
