#include <DB/Interpreters/Aggregator.h>

#include <DB/AggregateFunctions/AggregateFunctionCount.h>
#include <DB/AggregateFunctions/AggregateFunctionSum.h>
#include <DB/AggregateFunctions/AggregateFunctionAvg.h>
#include <DB/AggregateFunctions/AggregateFunctionsMinMaxAny.h>
#include <DB/AggregateFunctions/AggregateFunctionsArgMinMax.h>
#include <DB/AggregateFunctions/AggregateFunctionUniq.h>
#include <DB/AggregateFunctions/AggregateFunctionUniqUpTo.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantile.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantileTiming.h>
#include <DB/AggregateFunctions/AggregateFunctionIf.h>
#include <DB/AggregateFunctions/AggregateFunctionArray.h>
#include <DB/AggregateFunctions/AggregateFunctionState.h>
#include <DB/AggregateFunctions/AggregateFunctionMerge.h>


namespace DB
{


/** Шаблон цикла агрегации, позволяющий сгенерировать специализированный вариант для конкретной комбинации агрегатных функций.
  * Отличается от обычного тем, что вызовы агрегатных функций должны инлайниться, а цикл обновления агрегатных функций должен развернуться.
  *
  * Так как возможных комбинаций слишком много, то не представляется возможным сгенерировать их все заранее.
  * Этот шаблон предназначен для того, чтобы инстанцировать его в рантайме,
  *  путём запуска компилятора, компиляции shared library и использования её с помощью dlopen.
  */


/** Список типов - для удобного перечисления агрегатных функций.
  */
template <typename... TTail>
struct TypeList
{
	static constexpr size_t size = 0;

	template <size_t I>
	using At = std::nullptr_t;

	template <typename Func, size_t index = 0>
	static void forEach(Func && func)
	{
	}
};


template <typename THead, typename... TTail>
struct TypeList<THead, TTail...>
{
	using Head = THead;
	using Tail = TypeList<TTail...>;

	static constexpr size_t size = 1 + sizeof...(TTail);

	template <size_t I>
	using At = typename std::template conditional<I == 0, Head, typename Tail::template At<I - 1>>::type;

	template <typename Func, size_t index = 0>
	static void ALWAYS_INLINE forEach(Func && func)
	{
		func.template operator()<Head, index>();
		Tail::template forEach<Func, index + 1>(std::forward<Func>(func));
	}
};


struct AggregateFunctionsUpdater
{
	AggregateFunctionsUpdater(
		const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions_,
		const Sizes & offsets_of_aggregate_states_,
		Aggregator::AggregateColumns & aggregate_columns_,
		AggregateDataPtr & value_,
		size_t row_num_)
		: aggregate_functions(aggregate_functions_),
		offsets_of_aggregate_states(offsets_of_aggregate_states_),
		aggregate_columns(aggregate_columns_),
		value(value_), row_num(row_num_)
	{
	}

	template <typename AggregateFunction, size_t column_num>
	void operator()() ALWAYS_INLINE;

	const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions;
	const Sizes & offsets_of_aggregate_states;
	Aggregator::AggregateColumns & aggregate_columns;
	AggregateDataPtr & value;
	size_t row_num;
};

template <typename AggregateFunction, size_t column_num>
void AggregateFunctionsUpdater::operator()()
{
	static_cast<AggregateFunction *>(aggregate_functions[column_num])->add(
		value + offsets_of_aggregate_states[column_num],
		&aggregate_columns[column_num][0],
		row_num);
}

struct AggregateFunctionsCreator
{
	AggregateFunctionsCreator(
		const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions_,
		const Sizes & offsets_of_aggregate_states_,
		Aggregator::AggregateColumns & aggregate_columns_,
		AggregateDataPtr & aggregate_data_)
		: aggregate_functions(aggregate_functions_),
		offsets_of_aggregate_states(offsets_of_aggregate_states_),
		aggregate_data(aggregate_data_)
	{
	}

	template <typename AggregateFunction, size_t column_num>
	void operator()() ALWAYS_INLINE;

	const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions;
	const Sizes & offsets_of_aggregate_states;
	AggregateDataPtr & aggregate_data;
};

template <typename AggregateFunction, size_t column_num>
void AggregateFunctionsCreator::operator()()
{
	AggregateFunction * func = static_cast<AggregateFunction *>(aggregate_functions[column_num]);

	try
	{
		/** Может возникнуть исключение при нехватке памяти.
			* Для того, чтобы потом всё правильно уничтожилось, "откатываем" часть созданных состояний.
			* Код не очень удобный.
			*/
		func->create(aggregate_data + offsets_of_aggregate_states[column_num]);
	}
	catch (...)
	{
		for (size_t rollback_j = 0; rollback_j < column_num; ++rollback_j)
			func->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

		throw;
	}
}


template <typename Method, typename AggregateFunctionsList>
void NO_INLINE Aggregator::executeSpecialized(
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
		executeSpecializedCase<false, Method, AggregateFunctionsList>(
			method, state, aggregates_pool, rows, key_columns, aggregate_columns, key_sizes, keys, overflow_row);
	else
		executeSpecializedCase<true, Method, AggregateFunctionsList>(
			method, state, aggregates_pool, rows, key_columns, aggregate_columns, key_sizes, keys, overflow_row);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"

template <bool no_more_keys, typename Method, typename AggregateFunctionsList>
void NO_INLINE Aggregator::executeSpecializedCase(
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
					AggregateDataPtr value = Method::getAggregateData(it->second);

					/// Добавляем значения в агрегатные функции.
					AggregateFunctionsList::forEach(AggregateFunctionsUpdater(
						aggregate_functions, offsets_of_aggregate_states, aggregate_columns, value, i));

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
			aggregate_data = nullptr;

			method.onNewKey(*it, keys_size, i, keys, *aggregates_pool);

			AggregateDataPtr place = aggregates_pool->alloc(total_size_of_aggregate_states);

			AggregateFunctionsList::forEach(AggregateFunctionsCreator(
				aggregate_functions, offsets_of_aggregate_states, aggregate_columns, place));

			aggregate_data = place;
		}
		else
			method.onExistingKey(key, keys, *aggregates_pool);

		AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

		/// Добавляем значения в агрегатные функции.
		AggregateFunctionsList::forEach(AggregateFunctionsUpdater(
			aggregate_functions, offsets_of_aggregate_states, aggregate_columns, value, i));
	}
}

#pragma GCC diagnostic pop

template <typename AggregateFunctionsList>
void NO_INLINE Aggregator::executeSpecializedWithoutKey(
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
			AggregateFunctionsList::forEach(AggregateFunctionsUpdater(
				aggregate_functions, offsets_of_aggregate_states, aggregate_columns, res, i));
		}
	}
}

}


/** Основной код компилируется с помощью gcc 4.9.
  * Но SpecializedAggregator компилируется с помощью clang 3.6 в .so-файл.
  * Это делается потому что gcc не удаётся заставить инлайнить функции,
  *  которые были девиртуализированы, в конкретном случае, и производительность получается ниже.
  * А также clang проще распространять для выкладки на серверы.
  *
  * После перехода с gcc 4.8 и gnu++1x на gcc 4.9 и gnu++1y,
  *  при dlopen стала возникать ошибка: undefined symbol: __cxa_pure_virtual
  *
  * Скорее всего, это происходит из-за изменившейся версии этого символа:
  *  gcc создаёт в .so символ
  *   U __cxa_pure_virtual@@CXXABI_1.3
  *  а clang создаёт символ
  *   U __cxa_pure_virtual
  *
  * Но нам не принципиально, как будет реализована функция __cxa_pure_virtual,
  *  потому что она не вызывается при нормальной работе программы,
  *  а если вызывается - то программа и так гарантированно глючит.
  *
  * Поэтому, мы можем обойти проблему таким образом:
  */
extern "C" void __attribute__((__visibility__("default"), __noreturn__)) __cxa_pure_virtual() { abort(); };
