#include <Common/TypeList.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Aggregator.h>
#include <AggregateFunctions/AggregateFunctionCount.h>


namespace DB
{


/** An aggregation loop template that allows you to generate a custom variant for a specific combination of aggregate functions.
  * It differs from the usual one in that calls to aggregate functions should be inlined, and the update loop of the aggregate functions should be unrolled.
  *
  * Since there are too many possible combinations, it is not possible to generate them all in advance.
  * This template is intended to instantiate it in runtime,
  *  by running the compiler, compiling shared library, and using it with `dlopen`.
  */


struct AggregateFunctionsUpdater
{
    AggregateFunctionsUpdater(
        const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions_,
        const Sizes & offsets_of_aggregate_states_,
        Aggregator::AggregateColumns & aggregate_columns_,
        AggregateDataPtr & value_,
        size_t row_num_,
        Arena * arena_)
        : aggregate_functions(aggregate_functions_),
        offsets_of_aggregate_states(offsets_of_aggregate_states_),
        aggregate_columns(aggregate_columns_),
        value(value_), row_num(row_num_), arena(arena_)
    {
    }

    template <typename AggregateFunction, size_t column_num>
    void operator()() ALWAYS_INLINE;

    const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions;
    const Sizes & offsets_of_aggregate_states;
    Aggregator::AggregateColumns & aggregate_columns;
    AggregateDataPtr & value;
    size_t row_num;
    Arena * arena;
};

template <typename AggregateFunction, size_t column_num>
void AggregateFunctionsUpdater::operator()()
{
    static_cast<AggregateFunction *>(aggregate_functions[column_num])->add(
        value + offsets_of_aggregate_states[column_num],
        &aggregate_columns[column_num][0],
        row_num, arena);
}

struct AggregateFunctionsCreator
{
    AggregateFunctionsCreator(
        const Aggregator::AggregateFunctionsPlainPtrs & aggregate_functions_,
        const Sizes & offsets_of_aggregate_states_,
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
        /** An exception may occur if there is a shortage of memory.
          * To ensure that everything is properly destroyed, we "roll back" some of the created states.
          * The code is not very convenient.
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
    ColumnRawPtrs & key_columns,
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
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    const Sizes & key_sizes,
    StringRefs & keys,
    AggregateDataPtr overflow_row) const
{
    /// For all rows.
    typename Method::iterator it;
    typename Method::Key prev_key;
    for (size_t i = 0; i < rows; ++i)
    {
        bool inserted;            /// Inserted a new key, or was this key already?
        bool overflow = false;    /// New key did not fit in the hash table because of no_more_keys.

        /// Get the key to insert into the hash table.
        typename Method::Key key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *aggregates_pool);

        if (!no_more_keys)    /// Insert.
        {
            /// Optimization for frequently repeating keys.
            if (!Method::no_consecutive_keys_optimization)
            {
                if (i != 0 && key == prev_key)
                {
                    AggregateDataPtr value = Method::getAggregateData(it->second);

                    /// Add values into aggregate functions.
                    AggregateFunctionsList::forEach(AggregateFunctionsUpdater(
                        aggregate_functions, offsets_of_aggregate_states, aggregate_columns, value, i, aggregates_pool));

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
            /// Add only if the key already exists.
            inserted = false;
            it = method.data.find(key);
            if (method.data.end() == it)
                overflow = true;
        }

        /// If the key does not fit, and the data does not need to be aggregated in a separate row, then there's nothing to do.
        if (no_more_keys && overflow && !overflow_row)
        {
            method.onExistingKey(key, keys, *aggregates_pool);
            continue;
        }

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly some stuff related to the key.
        if (inserted)
        {
            AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
            aggregate_data = nullptr;

            method.onNewKey(*it, params.keys_size, keys, *aggregates_pool);

            AggregateDataPtr place = aggregates_pool->alloc(total_size_of_aggregate_states);

            AggregateFunctionsList::forEach(AggregateFunctionsCreator(
                aggregate_functions, offsets_of_aggregate_states, place));

            aggregate_data = place;
        }
        else
            method.onExistingKey(key, keys, *aggregates_pool);

        AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

        /// Add values into the aggregate functions.
        AggregateFunctionsList::forEach(AggregateFunctionsUpdater(
            aggregate_functions, offsets_of_aggregate_states, aggregate_columns, value, i, aggregates_pool));
    }
}

#pragma GCC diagnostic pop

template <typename AggregateFunctionsList>
void NO_INLINE Aggregator::executeSpecializedWithoutKey(
    AggregatedDataWithoutKey & res,
    size_t rows,
    AggregateColumns & aggregate_columns,
    Arena * arena) const
{
    /// Optimization in the case of a single aggregate function `count`.
    AggregateFunctionCount * agg_count = params.aggregates_size == 1
        ? typeid_cast<AggregateFunctionCount *>(aggregate_functions[0])
        : nullptr;

    if (agg_count)
        agg_count->addDelta(res, rows);
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            AggregateFunctionsList::forEach(AggregateFunctionsUpdater(
                aggregate_functions, offsets_of_aggregate_states, aggregate_columns, res, i, arena));
        }
    }
}

}


/** The main code is compiled with gcc 7.
  * But SpecializedAggregator is compiled using clang 6 into the .so file.
  * This is done because gcc can not get functions inlined,
  *  which were de-virtualized, in a particular case, and the performance is lower.
  * And also it's easier to distribute clang for deploy to the servers.
  *
  * After switching from gcc 4.8 and gnu++1x to gcc 4.9 and gnu++1y (and then to gcc 5),
  *  an error occurred with `dlopen`: undefined symbol: __cxa_pure_virtual
  *
  * Most likely, this is due to the changed version of this symbol:
  *  gcc creates a symbol in .so
  *   U __cxa_pure_virtual@@CXXABI_1.3
  *  but clang creates a symbol
  *   U __cxa_pure_virtual
  *
  * But it does not matter for us how the __cxa_pure_virtual function will be implemented,
  *  because it is not called during normal program execution,
  *  and if called - then the program is guaranteed buggy.
  *
  * Therefore, we can work around the problem this way
  */
extern "C" void __attribute__((__visibility__("default"), __noreturn__)) __cxa_pure_virtual() { abort(); }
