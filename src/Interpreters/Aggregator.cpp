#include <iomanip>
#include <thread>
#include <future>
#include <Poco/Version.h>
#include <Poco/Util/Application.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/Aggregator.h>
#include <Common/ClickHouseRevision.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <common/demangle.h>
#include <AggregateFunctions/AggregateFunctionArray.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/AggregateFunctionResample.h>
#include <Disks/StoragePolicy.h>
#include <IO/Operators.h>


namespace ProfileEvents
{
    extern const Event ExternalAggregationWritePart;
    extern const Event ExternalAggregationCompressedBytes;
    extern const Event ExternalAggregationUncompressedBytes;
}

namespace CurrentMetrics
{
    extern const Metric QueryThread;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int NOT_ENOUGH_SPACE;
    extern const int TOO_MANY_ROWS;
    extern const int EMPTY_DATA_PASSED;
    extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
    extern const int LOGICAL_ERROR;
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
            NAME ## _two_level = std::make_unique<decltype(NAME ## _two_level)::element_type>(*(NAME)); \
            (NAME).reset(); \
            type = Type::NAME ## _two_level; \
            break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M

        default:
            throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
    }
}

Block Aggregator::getHeader(bool final) const
{
    return params.getHeader(final);
}

Block Aggregator::Params::getHeader(
    const Block & src_header,
    const Block & intermediate_header,
    const ColumnNumbers & keys,
    const AggregateDescriptions & aggregates,
    bool final)
{
    Block res;

    if (src_header)
    {
        for (const auto & key : keys)
            res.insert(src_header.safeGetByPosition(key).cloneEmpty());

        for (const auto & aggregate : aggregates)
        {
            size_t arguments_size = aggregate.arguments.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = src_header.safeGetByPosition(aggregate.arguments[j]).type;

            DataTypePtr type;
            if (final)
                type = aggregate.function->getReturnType();
            else
                type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, argument_types, aggregate.parameters);

            res.insert({ type, aggregate.column_name });
        }
    }
    else if (intermediate_header)
    {
        res = intermediate_header.cloneEmpty();

        if (final)
        {
            for (const auto & aggregate : aggregates)
            {
                auto & elem = res.getByName(aggregate.column_name);

                elem.type = aggregate.function->getReturnType();
                elem.column = elem.type->createColumn();
            }
        }
    }

    return materializeBlock(res);
}

void Aggregator::Params::explain(WriteBuffer & out, size_t indent) const
{
    Strings res;
    const auto & header = src_header ? src_header
                                     : intermediate_header;

    String prefix(indent, ' ');

    {
        /// Dump keys.
        out << prefix << "Keys: ";

        bool first = true;
        for (auto key : keys)
        {
            if (!first)
                out << ", ";
            first = false;

            if (key >= header.columns())
                out << "unknown position " << key;
            else
                out << header.getByPosition(key).name;
        }

        out << '\n';
    }

    if (!aggregates.empty())
    {
        out << prefix << "Aggregates:\n";

        for (const auto & aggregate : aggregates)
            aggregate.explain(out, indent + 4);
    }
}

Aggregator::Aggregator(const Params & params_)
    : params(params_),
    isCancelled([]() { return false; })
{
    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            memory_usage_before_aggregation = memory_tracker->get();

    aggregate_functions.resize(params.aggregates_size);
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i] = params.aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params.aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    // aggregate_states will be aligned as below:
    // |<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    //
    // pad_N will be used to match alignment requirement for each next state.
    // The address of state_1 is aligned based on maximum alignment requirements in states
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        offsets_of_aggregate_states[i] = total_size_of_aggregate_states;

        total_size_of_aggregate_states += params.aggregates[i].function->sizeOfData();

        // aggregate states are aligned based on maximum requirement
        align_aggregate_states = std::max(align_aggregate_states, params.aggregates[i].function->alignOfData());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < params.aggregates_size)
        {
            size_t alignment_of_next_state = params.aggregates[i + 1].function->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception("Logical error: alignOfData is not 2^N", ErrorCodes::LOGICAL_ERROR);

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            total_size_of_aggregate_states = (total_size_of_aggregate_states + alignment_of_next_state - 1) / alignment_of_next_state * alignment_of_next_state;
        }

        if (!params.aggregates[i].function->hasTrivialDestructor())
            all_aggregates_has_trivial_destructor = false;
    }

    method_chosen = chooseAggregationMethod();
    HashMethodContext::Settings cache_settings;
    cache_settings.max_threads = params.max_threads;
    aggregation_state_cache = AggregatedDataVariants::createCache(method_chosen, cache_settings);
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod()
{
    /// If no keys. All aggregating to single row.
    if (params.keys_size == 0)
        return AggregatedDataVariants::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(params.keys.size());
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (const auto & pos : params.keys)
    {
        DataTypePtr type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(pos).type;

        if (type->lowCardinality())
        {
            has_low_cardinality = true;
            type = removeLowCardinality(type);
        }

        if (type->isNullable())
        {
            has_nullable_key = true;
            type = removeNullable(type);
        }

        types_removed_nullable.push_back(type);
    }

    /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    key_sizes.resize(params.keys_size);
    for (size_t j = 0; j < params.keys_size; ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                ++num_fixed_contiguous_keys;
                key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += key_sizes[j];
            }
        }
    }

    if (has_nullable_key)
    {
        if (params.keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return AggregatedDataVariants::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<DummyUInt256>>::value + keys_bytes <= 32)
                return AggregatedDataVariants::Type::nullable_keys256;
        }

        if (has_low_cardinality && params.keys_size == 1)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 1)
                    return AggregatedDataVariants::Type::low_cardinality_key8;
                if (size_of_field == 2)
                    return AggregatedDataVariants::Type::low_cardinality_key16;
                if (size_of_field == 4)
                    return AggregatedDataVariants::Type::low_cardinality_key32;
                if (size_of_field == 8)
                    return AggregatedDataVariants::Type::low_cardinality_key64;
            }
            else if (isString(types_removed_nullable[0]))
                return AggregatedDataVariants::Type::low_cardinality_key_string;
            else if (isFixedString(types_removed_nullable[0]))
                return AggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        }

        /// Fallback case.
        return AggregatedDataVariants::Type::serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params.keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (has_low_cardinality)
        {
            if (size_of_field == 1)
                return AggregatedDataVariants::Type::low_cardinality_key8;
            if (size_of_field == 2)
                return AggregatedDataVariants::Type::low_cardinality_key16;
            if (size_of_field == 4)
                return AggregatedDataVariants::Type::low_cardinality_key32;
            if (size_of_field == 8)
                return AggregatedDataVariants::Type::low_cardinality_key64;
        }

        if (size_of_field == 1)
            return AggregatedDataVariants::Type::key8;
        if (size_of_field == 2)
            return AggregatedDataVariants::Type::key16;
        if (size_of_field == 4)
            return AggregatedDataVariants::Type::key32;
        if (size_of_field == 8)
            return AggregatedDataVariants::Type::key64;
        if (size_of_field == 16)
            return AggregatedDataVariants::Type::keys128;
        if (size_of_field == 32)
            return AggregatedDataVariants::Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    if (params.keys_size == 1 && isFixedString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return AggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        else
            return AggregatedDataVariants::Type::key_fixed_string;
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params.keys_size == num_fixed_contiguous_keys)
    {
        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return AggregatedDataVariants::Type::low_cardinality_keys128;
            if (keys_bytes <= 32)
                return AggregatedDataVariants::Type::low_cardinality_keys256;
        }

        if (keys_bytes <= 2)
            return AggregatedDataVariants::Type::keys16;
        if (keys_bytes <= 4)
            return AggregatedDataVariants::Type::keys32;
        if (keys_bytes <= 8)
            return AggregatedDataVariants::Type::keys64;
        if (keys_bytes <= 16)
            return AggregatedDataVariants::Type::keys128;
        if (keys_bytes <= 32)
            return AggregatedDataVariants::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (params.keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return AggregatedDataVariants::Type::low_cardinality_key_string;
        else
            return AggregatedDataVariants::Type::key_string;
    }

    return AggregatedDataVariants::Type::serialized;
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        try
        {
            /** An exception may occur if there is a shortage of memory.
              * In order that then everything is properly destroyed, we "roll back" some of the created states.
              * The code is not very convenient.
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


/** It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
  * (Probably because after the inline of this function, more internal functions no longer be inlined.)
  * Inline does not make sense, since the inner loop is entirely inside this function.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
    Method & method,
    Arena * aggregates_pool,
    size_t rows,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    bool no_more_keys,
    AggregateDataPtr overflow_row) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    if (!no_more_keys)
        executeImplBatch(method, state, aggregates_pool, rows, aggregate_instructions);
    else
        executeImplCase<true>(method, state, aggregates_pool, rows, aggregate_instructions, overflow_row);
}


template <bool no_more_keys, typename Method>
void NO_INLINE Aggregator::executeImplCase(
    Method & method,
    typename Method::State & state,
    Arena * aggregates_pool,
    size_t rows,
    AggregateFunctionInstruction * aggregate_instructions,
    AggregateDataPtr overflow_row) const
{
    /// NOTE When editing this code, also pay attention to SpecializedAggregator.h.

    /// For all rows.
    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        if constexpr (!no_more_keys)  /// Insert.
        {
            auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool);

            /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
            if (emplace_result.isInserted())
            {
                /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                emplace_result.setMapped(nullptr);

                aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(aggregate_data);

                emplace_result.setMapped(aggregate_data);
            }
            else
                aggregate_data = emplace_result.getMapped();
        }
        else
        {
            /// Add only if the key already exists.
            auto find_result = state.findKey(method.data, i, *aggregates_pool);
            if (find_result.isFound())
                aggregate_data = find_result.getMapped();
        }

        /// aggregate_date == nullptr means that the new key did not fit in the hash table because of no_more_keys.

        /// If the key does not fit, and the data does not need to be aggregated in a separate row, then there's nothing to do.
        if (!aggregate_data && !overflow_row)
            continue;

        AggregateDataPtr value = aggregate_data ? aggregate_data : overflow_row;

        /// Add values to the aggregate functions.
        for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
            (*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i, aggregates_pool);
    }
}


template <typename Method>
void NO_INLINE Aggregator::executeImplBatch(
    Method & method,
    typename Method::State & state,
    Arena * aggregates_pool,
    size_t rows,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    /// Optimization for special case when there are no aggregate functions.
    if (params.aggregates_size == 0)
    {
        /// For all rows.
        AggregateDataPtr place = aggregates_pool->alloc(0);
        for (size_t i = 0; i < rows; ++i)
            state.emplaceKey(method.data, i, *aggregates_pool).setMapped(place);
        return;
    }

    /// Optimization for special case when aggregating by 8bit key.
    if constexpr (std::is_same_v<Method, typename decltype(AggregatedDataVariants::key8)::element_type>)
    {
        /// We use another method if there are aggregate functions with -Array combinator.
        bool has_arrays = false;
        for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
        {
            if (inst->offsets)
            {
                has_arrays = true;
                break;
            }
        }

        if (!has_arrays)
        {
            for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
            {
                inst->batch_that->addBatchLookupTable8(
                    rows,
                    reinterpret_cast<AggregateDataPtr *>(method.data.data()),
                    inst->state_offset,
                    [&](AggregateDataPtr & aggregate_data)
                    {
                        aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(aggregate_data);
                    },
                    state.getKeyData(),
                    inst->batch_arguments,
                    aggregates_pool);
            }
            return;
        }
    }

    /// Generic case.

    PODArray<AggregateDataPtr> places(rows);

    /// For all rows.
    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted())
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);

            emplace_result.setMapped(aggregate_data);
        }
        else
            aggregate_data = emplace_result.getMapped();

        places[i] = aggregate_data;
        assert(places[i] != nullptr);
    }

    /// Add values to the aggregate functions.
    for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
    {
        if (inst->offsets)
            inst->batch_that->addBatchArray(rows, places.data(), inst->state_offset, inst->batch_arguments, inst->offsets, aggregates_pool);
        else
            inst->batch_that->addBatch(rows, places.data(), inst->state_offset, inst->batch_arguments, aggregates_pool);
    }
}


void NO_INLINE Aggregator::executeWithoutKeyImpl(
    AggregatedDataWithoutKey & res,
    size_t rows,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena)
{
    /// Adding values
    for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
    {
        if (inst->offsets)
            inst->batch_that->addBatchSinglePlace(
                inst->offsets[static_cast<ssize_t>(rows - 1)], res + inst->state_offset, inst->batch_arguments, arena);
        else
            inst->batch_that->addBatchSinglePlace(rows, res + inst->state_offset, inst->batch_arguments, arena);
    }
}


void NO_INLINE Aggregator::executeOnIntervalWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena)
{
    /// Adding values
    for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
    {
        if (inst->offsets)
            inst->batch_that->addBatchSinglePlaceFromInterval(inst->offsets[row_begin], inst->offsets[row_end - 1], res + inst->state_offset, inst->batch_arguments, arena);
        else
            inst->batch_that->addBatchSinglePlaceFromInterval(row_begin, row_end, res + inst->state_offset, inst->batch_arguments, arena);
    }
}


void Aggregator::prepareAggregateInstructions(Columns columns, AggregateColumns & aggregate_columns, Columns & materialized_columns,
    AggregateFunctionInstructions & aggregate_functions_instructions, NestedColumnsHolder & nested_columns_holder)
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i].resize(params.aggregates[i].arguments.size());

    aggregate_functions_instructions.resize(params.aggregates_size + 1);
    aggregate_functions_instructions[params.aggregates_size].that = nullptr;

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            materialized_columns.push_back(columns.at(params.aggregates[i].arguments[j])->convertToFullColumnIfConst());
            aggregate_columns[i][j] = materialized_columns.back().get();

            auto column_no_lc = recursiveRemoveLowCardinality(aggregate_columns[i][j]->getPtr());
            if (column_no_lc.get() != aggregate_columns[i][j])
            {
                materialized_columns.emplace_back(std::move(column_no_lc));
                aggregate_columns[i][j] = materialized_columns.back().get();
            }
        }

        aggregate_functions_instructions[i].arguments = aggregate_columns[i].data();
        aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];
        auto * that = aggregate_functions[i];
        /// Unnest consecutive trailing -State combinators
        while (const auto * func = typeid_cast<const AggregateFunctionState *>(that))
            that = func->getNestedFunction().get();
        aggregate_functions_instructions[i].that = that;
        aggregate_functions_instructions[i].func = that->getAddressOfAddFunction();

        if (const auto * func = typeid_cast<const AggregateFunctionArray *>(that))
        {
            /// Unnest consecutive -State combinators before -Array
            that = func->getNestedFunction().get();
            while (const auto * nested_func = typeid_cast<const AggregateFunctionState *>(that))
                that = nested_func->getNestedFunction().get();
            auto [nested_columns, offsets] = checkAndGetNestedArrayOffset(aggregate_columns[i].data(), that->getArgumentTypes().size());
            nested_columns_holder.push_back(std::move(nested_columns));
            aggregate_functions_instructions[i].batch_arguments = nested_columns_holder.back().data();
            aggregate_functions_instructions[i].offsets = offsets;
        }
        else
            aggregate_functions_instructions[i].batch_arguments = aggregate_columns[i].data();

        aggregate_functions_instructions[i].batch_that = that;
    }
}


bool Aggregator::executeOnBlock(const Block & block, AggregatedDataVariants & result,
                                ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns, bool & no_more_keys)
{
    UInt64 num_rows = block.rows();
    return executeOnBlock(block.getColumns(), num_rows, result, key_columns, aggregate_columns, no_more_keys);
}


bool Aggregator::executeOnBlock(Columns columns, UInt64 num_rows, AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns, bool & no_more_keys)
{
    if (isCancelled())
        return true;

    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        result.init(method_chosen);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
        LOG_TRACE(log, "Aggregation method: {}", result.getMethodName());
    }

    if (isCancelled())
        return true;

    /** Constant columns are not supported directly during aggregation.
      * To make them work anyway, we materialize them.
      */
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
    {
        materialized_columns.push_back(columns.at(params.keys[i])->convertToFullColumnIfConst());
        key_columns[i] = materialized_columns.back().get();

        if (!result.isLowCardinality())
        {
            auto column_no_lc = recursiveRemoveLowCardinality(key_columns[i]->getPtr());
            if (column_no_lc.get() != key_columns[i])
            {
                materialized_columns.emplace_back(std::move(column_no_lc));
                key_columns[i] = materialized_columns.back().get();
            }
        }
    }
    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    if (isCancelled())
        return true;

    if ((params.overflow_row || result.type == AggregatedDataVariants::Type::without_key) && !result.without_key)
    {
        AggregateDataPtr place = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        result.without_key = place;
    }

    /// We select one of the aggregation methods and call it.

    /// For the case when there are no keys (all aggregate into one row).
    if (result.type == AggregatedDataVariants::Type::without_key)
    {
        executeWithoutKeyImpl(result.without_key, num_rows, aggregate_functions_instructions.data(), result.aggregates_pool);
    }
    else
    {
        /// This is where data is written that does not fit in `max_rows_to_group_by` with `group_by_overflow_mode = any`.
        AggregateDataPtr overflow_row_ptr = params.overflow_row ? result.without_key : nullptr;

        #define M(NAME, IS_TWO_LEVEL) \
            else if (result.type == AggregatedDataVariants::Type::NAME) \
                executeImpl(*result.NAME, result.aggregates_pool, num_rows, key_columns, aggregate_functions_instructions.data(), \
                    no_more_keys, overflow_row_ptr);

        if (false) {} // NOLINT
        APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
    }

    size_t result_size = result.sizeWithoutOverflowRow();
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            current_memory_usage = memory_tracker->get();

    /// Here all the results in the sum are taken into account, from different threads.
    auto result_size_bytes = current_memory_usage - memory_usage_before_aggregation;

    bool worth_convert_to_two_level
        = (params.group_by_two_level_threshold && result_size >= params.group_by_two_level_threshold)
        || (params.group_by_two_level_threshold_bytes && result_size_bytes >= static_cast<Int64>(params.group_by_two_level_threshold_bytes));

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
        result.convertToTwoLevel();

    /// Checking the constraints.
    if (!checkLimits(result_size, no_more_keys))
        return false;

    /** Flush data to disk if too much RAM is consumed.
      * Data can only be flushed to disk if a two-level aggregation structure is used.
      */
    if (params.max_bytes_before_external_group_by
        && result.isTwoLevel()
        && current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by)
        && worth_convert_to_two_level)
    {
        size_t size = current_memory_usage + params.min_free_disk_space;

        std::string tmp_path = params.tmp_volume->getDisk()->getPath();

        // enoughSpaceInDirectory() is not enough to make it right, since
        // another process (or another thread of aggregator) can consume all
        // space.
        //
        // But true reservation (IVolume::reserve()) cannot be used here since
        // current_memory_usage does not takes compression into account and
        // will reserve way more that actually will be used.
        //
        // Hence let's do a simple check.
        if (!enoughSpaceInDirectory(tmp_path, size))
            throw Exception("Not enough space for external aggregation in " + tmp_path, ErrorCodes::NOT_ENOUGH_SPACE);

        writeToTemporaryFile(result, tmp_path);
    }

    return true;
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants, const String & tmp_path)
{
    Stopwatch watch;
    size_t rows = data_variants.size();

    auto file = createTemporaryFile(tmp_path);
    const std::string & path = file->path();
    WriteBufferFromFile file_buf(path);
    CompressedWriteBuffer compressed_buf(file_buf);
    NativeBlockOutputStream block_out(compressed_buf, DBMS_TCP_PROTOCOL_VERSION, getHeader(false));

    LOG_DEBUG(log, "Writing part of aggregation data into temporary file {}.", path);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationWritePart);

    /// Flush only two-level data and possibly overflow data.

#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        writeToTemporaryFileImpl(data_variants, *data_variants.NAME, block_out);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    /// NOTE Instead of freeing up memory and creating new hash tables and arenas, you can re-use the old ones.
    data_variants.init(data_variants.type);
    data_variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();
    data_variants.without_key = nullptr;

    block_out.flush();
    compressed_buf.next();
    file_buf.next();

    double elapsed_seconds = watch.elapsedSeconds();
    double compressed_bytes = file_buf.count();
    double uncompressed_bytes = compressed_buf.count();

    {
        std::lock_guard lock(temporary_files.mutex);
        temporary_files.files.emplace_back(std::move(file));
        temporary_files.sum_size_uncompressed += uncompressed_bytes;
        temporary_files.sum_size_compressed += compressed_bytes;
    }

    ProfileEvents::increment(ProfileEvents::ExternalAggregationCompressedBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationUncompressedBytes, uncompressed_bytes);

    LOG_TRACE(log,
        "Written part in {} sec., {} rows, {} uncompressed, {} compressed,"
        " {} uncompressed bytes per row, {} compressed bytes per row, compression rate: {}"
        " ({} rows/sec., {}/sec. uncompressed, {}/sec. compressed)",
        elapsed_seconds,
        rows,
        ReadableSize(uncompressed_bytes),
        ReadableSize(compressed_bytes),
        uncompressed_bytes / rows,
        compressed_bytes / rows,
        uncompressed_bytes / compressed_bytes,
        rows / elapsed_seconds,
        ReadableSize(uncompressed_bytes / elapsed_seconds),
        ReadableSize(compressed_bytes / elapsed_seconds));
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants)
{
    String tmp_path = params.tmp_volume->getDisk()->getPath();
    return writeToTemporaryFile(data_variants, tmp_path);
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
            MutableColumns & key_columns,
            AggregateColumnsData & aggregate_columns,
            MutableColumns & final_aggregate_columns,
            Arena * arena,
            bool final_)
        {
            convertToBlockImpl(method, method.data.impls[bucket],
                key_columns, aggregate_columns, final_aggregate_columns, arena, final_);
        });

    block.info.bucket_num = bucket;
    return block;
}

Block Aggregator::mergeAndConvertOneBucketToBlock(
    ManyAggregatedDataVariants & variants,
    Arena * arena,
    bool final,
    size_t bucket,
    std::atomic<bool> * is_cancelled) const
{
    auto & merged_data = *variants[0];
    auto method = merged_data.type;
    Block block;

    if (false) {} // NOLINT
#define M(NAME) \
    else if (method == AggregatedDataVariants::Type::NAME) \
    { \
        mergeBucketImpl<decltype(merged_data.NAME)::element_type>(variants, bucket, arena); \
        if (is_cancelled && is_cancelled->load(std::memory_order_seq_cst)) \
            return {}; \
        block = convertOneBucketToBlock(merged_data, *merged_data.NAME, final, bucket); \
    }

    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M

    return block;
}


template <typename Method>
void Aggregator::writeToTemporaryFileImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    IBlockOutputStream & out)
{
    size_t max_temporary_block_size_rows = 0;
    size_t max_temporary_block_size_bytes = 0;

    auto update_max_sizes = [&](const Block & block)
    {
        size_t block_size_rows = block.rows();
        size_t block_size_bytes = block.bytes();

        if (block_size_rows > max_temporary_block_size_rows)
            max_temporary_block_size_rows = block_size_rows;
        if (block_size_bytes > max_temporary_block_size_bytes)
            max_temporary_block_size_bytes = block_size_bytes;
    };

    for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
    {
        Block block = convertOneBucketToBlock(data_variants, method, false, bucket);
        out.write(block);
        update_max_sizes(block);
    }

    if (params.overflow_row)
    {
        Block block = prepareBlockAndFillWithoutKey(data_variants, false, true);
        out.write(block);
        update_max_sizes(block);
    }

    /// Pass ownership of the aggregate functions states:
    /// `data_variants` will not destroy them in the destructor, they are now owned by ColumnAggregateFunction objects.
    data_variants.aggregator = nullptr;

    LOG_TRACE(log, "Max size of temporary block: {} rows, {}.", max_temporary_block_size_rows, ReadableSize(max_temporary_block_size_bytes));
}


bool Aggregator::checkLimits(size_t result_size, bool & no_more_keys) const
{
    if (!no_more_keys && params.max_rows_to_group_by && result_size > params.max_rows_to_group_by)
    {
        switch (params.group_by_overflow_mode)
        {
            case OverflowMode::THROW:
                throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
                    + " rows, maximum: " + toString(params.max_rows_to_group_by),
                    ErrorCodes::TOO_MANY_ROWS);

            case OverflowMode::BREAK:
                return false;

            case OverflowMode::ANY:
                no_more_keys = true;
                break;
        }
    }

    return true;
}


void Aggregator::execute(const BlockInputStreamPtr & stream, AggregatedDataVariants & result)
{
    if (isCancelled())
        return;

    ColumnRawPtrs key_columns(params.keys_size);
    AggregateColumns aggregate_columns(params.aggregates_size);

    /** Used if there is a limit on the maximum number of rows in the aggregation,
      *  and if group_by_overflow_mode == ANY.
      * In this case, new keys are not added to the set, but aggregation is performed only by
      *  keys that have already managed to get into the set.
      */
    bool no_more_keys = false;

    LOG_TRACE(log, "Aggregating");

    Stopwatch watch;

    size_t src_rows = 0;
    size_t src_bytes = 0;

    /// Read all the data
    while (Block block = stream->read())
    {
        if (isCancelled())
            return;

        src_rows += block.rows();
        src_bytes += block.bytes();

        if (!executeOnBlock(block, result, key_columns, aggregate_columns, no_more_keys))
            break;
    }

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (result.empty() && params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
        executeOnBlock(stream->getHeader(), result, key_columns, aggregate_columns, no_more_keys);

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = result.sizeWithoutOverflowRow();

    LOG_TRACE(log, "Aggregated. {} to {} rows (from {}) in {} sec. ({} rows/sec., {}/sec.)",
        src_rows, rows, ReadableSize(src_bytes),
        elapsed_seconds, src_rows / elapsed_seconds,
        ReadableSize(src_bytes / elapsed_seconds));
}


template <typename Method, typename Table>
void Aggregator::convertToBlockImpl(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    AggregateColumnsData & aggregate_columns,
    MutableColumns & final_aggregate_columns,
    Arena * arena,
    bool final) const
{
    if (data.empty())
        return;

    if (key_columns.size() != params.keys_size)
        throw Exception{"Aggregate. Unexpected key columns size.", ErrorCodes::LOGICAL_ERROR};

    if (final)
        convertToBlockImplFinal(method, data, key_columns, final_aggregate_columns, arena);
    else
        convertToBlockImplNotFinal(method, data, key_columns, aggregate_columns);
    /// In order to release memory early.
    data.clearAndShrink();
}


template <typename Mapped>
inline void Aggregator::insertAggregatesIntoColumns(
    Mapped & mapped,
    MutableColumns & final_aggregate_columns,
    Arena * arena) const
{
    /** Final values of aggregate functions are inserted to columns.
      * Then states of aggregate functions, that are not longer needed, are destroyed.
      *
      * We mark already destroyed states with "nullptr" in data,
      *  so they will not be destroyed in destructor of Aggregator
      * (other values will be destroyed in destructor in case of exception).
      *
      * But it becomes tricky, because we have multiple aggregate states pointed by a single pointer in data.
      * So, if exception is thrown in the middle of moving states for different aggregate functions,
      *  we have to catch exceptions and destroy all the states that are no longer needed,
      *  to keep the data in consistent state.
      *
      * It is also tricky, because there are aggregate functions with "-State" modifier.
      * When we call "insertResultInto" for them, they insert a pointer to the state to ColumnAggregateFunction
      *  and ColumnAggregateFunction will take ownership of this state.
      * So, for aggregate functions with "-State" modifier, the state must not be destroyed
      *  after it has been transferred to ColumnAggregateFunction.
      * But we should mark that the data no longer owns these states.
      */

    size_t insert_i = 0;
    std::exception_ptr exception;

    try
    {
        /// Insert final values of aggregate functions into columns.
        for (; insert_i < params.aggregates_size; ++insert_i)
            aggregate_functions[insert_i]->insertResultInto(
                mapped + offsets_of_aggregate_states[insert_i],
                *final_aggregate_columns[insert_i],
                arena);
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /** Destroy states that are no longer needed. This loop does not throw.
        *
        * Don't destroy states for "-State" aggregate functions,
        *  because the ownership of this state is transferred to ColumnAggregateFunction
        *  and ColumnAggregateFunction will take care.
        *
        * But it's only for states that has been transferred to ColumnAggregateFunction
        *  before exception has been thrown;
        */
    for (size_t destroy_i = 0; destroy_i < params.aggregates_size; ++destroy_i)
    {
        /// If ownership was not transferred to ColumnAggregateFunction.
        if (!(destroy_i < insert_i && aggregate_functions[destroy_i]->isState()))
            aggregate_functions[destroy_i]->destroy(
                mapped + offsets_of_aggregate_states[destroy_i]);
    }

    /// Mark the cell as destroyed so it will not be destroyed in destructor.
    mapped = nullptr;

    if (exception)
        std::rethrow_exception(exception);
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplFinal(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    MutableColumns & final_aggregate_columns,
    Arena * arena) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (data.hasNullKeyData())
        {
            key_columns[0]->insertDefault();
            insertAggregatesIntoColumns(data.getNullKeyData(), final_aggregate_columns, arena);
        }
    }

    data.forEachValue([&](const auto & key, auto & mapped)
    {
        method.insertKeyIntoColumns(key, key_columns, key_sizes);
        insertAggregatesIntoColumns(mapped, final_aggregate_columns, arena);
    });
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    AggregateColumnsData & aggregate_columns) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (data.hasNullKeyData())
        {
            key_columns[0]->insertDefault();

            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_columns[i]->push_back(data.getNullKeyData() + offsets_of_aggregate_states[i]);

            data.getNullKeyData() = nullptr;
        }
    }

    data.forEachValue([&](const auto & key, auto & mapped)
    {
        method.insertKeyIntoColumns(key, key_columns, key_sizes);

        /// reserved, so push_back does not throw exceptions
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_columns[i]->push_back(mapped + offsets_of_aggregate_states[i]);

        mapped = nullptr;
    });
}


template <typename Filler>
Block Aggregator::prepareBlockAndFill(
    AggregatedDataVariants & data_variants,
    bool final,
    size_t rows,
    Filler && filler) const
{
    MutableColumns key_columns(params.keys_size);
    MutableColumns aggregate_columns(params.aggregates_size);
    MutableColumns final_aggregate_columns(params.aggregates_size);
    AggregateColumnsData aggregate_columns_data(params.aggregates_size);

    Block header = getHeader(final);

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        key_columns[i] = header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (!final)
        {
            const auto & aggregate_column_name = params.aggregates[i].column_name;
            aggregate_columns[i] = header.getByName(aggregate_column_name).type->createColumn();

            /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
            ColumnAggregateFunction & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

            for (auto & pool : data_variants.aggregates_pools)
                column_aggregate_func.addArena(pool);

            aggregate_columns_data[i] = &column_aggregate_func.getData();
            aggregate_columns_data[i]->reserve(rows);
        }
        else
        {
            final_aggregate_columns[i] = aggregate_functions[i]->getReturnType()->createColumn();
            final_aggregate_columns[i]->reserve(rows);

            if (aggregate_functions[i]->isState())
            {
                /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                if (auto * column_aggregate_func = typeid_cast<ColumnAggregateFunction *>(final_aggregate_columns[i].get()))
                    for (auto & pool : data_variants.aggregates_pools)
                        column_aggregate_func->addArena(pool);

                /// Aggregate state can be wrapped into array if aggregate function ends with -Resample combinator.
                final_aggregate_columns[i]->forEachSubcolumn([&data_variants](auto & subcolumn)
                {
                    if (auto * column_aggregate_func = typeid_cast<ColumnAggregateFunction *>(subcolumn.get()))
                        for (auto & pool : data_variants.aggregates_pools)
                            column_aggregate_func->addArena(pool);
                });
            }
        }
    }

    filler(key_columns, aggregate_columns_data, final_aggregate_columns, data_variants.aggregates_pool, final);

    Block res = header.cloneEmpty();

    for (size_t i = 0; i < params.keys_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        if (final)
            res.getByName(aggregate_column_name).column = std::move(final_aggregate_columns[i]);
        else
            res.getByName(aggregate_column_name).column = std::move(aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = header.columns();
    for (size_t i = 0; i < columns; ++i)
        if (isColumnConst(*res.getByPosition(i).column))
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}

void Aggregator::fillAggregateColumnsWithSingleKey(
    AggregatedDataVariants & data_variants,
    MutableColumns & final_aggregate_columns)
{
    AggregatedDataWithoutKey & data = data_variants.without_key;

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        ColumnAggregateFunction & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*final_aggregate_columns[i]);
        for (auto & pool : data_variants.aggregates_pools)
        {
            column_aggregate_func.addArena(pool);
        }
        column_aggregate_func.getData().push_back(data + offsets_of_aggregate_states[i]);
    }
    data = nullptr;
}

void Aggregator::createStatesAndFillKeyColumnsWithSingleKey(
    AggregatedDataVariants & data_variants,
    Columns & key_columns,
    size_t key_row,
    MutableColumns & final_key_columns)
{
    AggregateDataPtr place = data_variants.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    createAggregateStates(place);
    data_variants.without_key = place;

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        final_key_columns[i]->insertFrom(*key_columns[i].get(), key_row);
    }
}

Block Aggregator::prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const
{
    size_t rows = 1;

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        bool final_)
    {
        if (data_variants.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
        {
            AggregatedDataWithoutKey & data = data_variants.without_key;

            if (!final_)
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_columns[i]->push_back(data + offsets_of_aggregate_states[i]);
                data = nullptr;
            }
            else
            {
                insertAggregatesIntoColumns(data, final_aggregate_columns, arena);
            }

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

    return block;
}

Block Aggregator::prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = data_variants.sizeWithoutOverflowRow();

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        bool final_)
    {
    #define M(NAME) \
        else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
            convertToBlockImpl(*data_variants.NAME, data_variants.NAME->data, \
                key_columns, aggregate_columns, final_aggregate_columns, arena, final_);

        if (false) {} // NOLINT
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
    #undef M
        else
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    };

    return prepareBlockAndFill(data_variants, final, rows, filler);
}


BlocksList Aggregator::prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, ThreadPool * thread_pool) const
{
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, final, thread_pool);

    if (false) {} // NOLINT
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
    ThreadPool * thread_pool) const
{
    auto converter = [&](size_t bucket, ThreadGroupStatusPtr thread_group)
    {
        if (thread_group)
            CurrentThread::attachToIfDetached(thread_group);
        return convertOneBucketToBlock(data_variants, method, final, bucket);
    };

    /// packaged_task is used to ensure that exceptions are automatically thrown into the main stream.

    std::vector<std::packaged_task<Block()>> tasks(Method::Data::NUM_BUCKETS);

    try
    {
        for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
        {
            if (method.data.impls[bucket].empty())
                continue;

            tasks[bucket] = std::packaged_task<Block()>([group = CurrentThread::getGroup(), bucket, &converter]{ return converter(bucket, group); });

            if (thread_pool)
                thread_pool->scheduleOrThrowOnError([bucket, &tasks] { tasks[bucket](); });
            else
                tasks[bucket]();
        }
    }
    catch (...)
    {
        /// If this is not done, then in case of an exception, tasks will be destroyed before the threads are completed, and it will be bad.
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

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000  /// TODO Make a custom threshold.
        && data_variants.isTwoLevel())                      /// TODO Use the shared thread pool with the `merge` function.
        thread_pool = std::make_unique<ThreadPool>(max_threads);

    if (isCancelled())
        return BlocksList();

    if (data_variants.without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(
            data_variants, final, data_variants.type != AggregatedDataVariants::Type::without_key));

    if (isCancelled())
        return BlocksList();

    if (data_variants.type != AggregatedDataVariants::Type::without_key)
    {
        if (!data_variants.isTwoLevel())
            blocks.emplace_back(prepareBlockAndFillSingleLevel(data_variants, final));
        else
            blocks.splice(blocks.end(), prepareBlocksAndFillTwoLevel(data_variants, final, thread_pool.get()));
    }

    if (!final)
    {
        /// data_variants will not destroy the states of aggregate functions in the destructor.
        /// Now ColumnAggregateFunction owns the states.
        data_variants.aggregator = nullptr;
    }

    if (isCancelled())
        return BlocksList();

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_TRACE(log,
        "Converted aggregated data to blocks. {} rows, {} in {} sec. ({} rows/sec., {}/sec.)",
        rows, ReadableSize(bytes),
        elapsed_seconds, rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    return blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNullKey(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (table_src.hasNullKeyData())
        {
            if (!table_dst.hasNullKeyData())
            {
                table_dst.hasNullKeyData() = true;
                table_dst.getNullKeyData() = table_src.getNullKeyData();
            }
            else
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->merge(
                            table_dst.getNullKeyData() + offsets_of_aggregate_states[i],
                            table_src.getNullKeyData() + offsets_of_aggregate_states[i],
                            arena);

                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->destroy(
                            table_src.getNullKeyData() + offsets_of_aggregate_states[i]);
            }

            table_src.hasNullKeyData() = false;
            table_src.getNullKeyData() = nullptr;
        }
    }
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

    table_src.mergeToViaEmplace(table_dst,
        [&](AggregateDataPtr & dst, AggregateDataPtr & src, bool inserted)
    {
        if (!inserted)
        {
            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_functions[i]->merge(
                    dst + offsets_of_aggregate_states[i],
                    src + offsets_of_aggregate_states[i],
                    arena);

            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);
        }
        else
        {
            dst = src;
        }

        src = nullptr;
    });
    table_src.clearAndShrink();
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNoMoreKeysImpl(
    Table & table_dst,
    AggregatedDataWithoutKey & overflows,
    Table & table_src,
    Arena * arena) const
{
    /// Note : will create data for NULL key if not exist
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

    table_src.mergeToViaFind(table_dst, [&](AggregateDataPtr dst, AggregateDataPtr & src, bool found)
    {
        AggregateDataPtr res_data = found ? dst : overflows;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                res_data + offsets_of_aggregate_states[i],
                src + offsets_of_aggregate_states[i],
                arena);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);

        src = nullptr;
    });
    table_src.clearAndShrink();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataOnlyExistingKeysImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    /// Note : will create data for NULL key if not exist
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

    table_src.mergeToViaFind(table_dst,
        [&](AggregateDataPtr dst, AggregateDataPtr & src, bool found)
    {
        if (!found)
            return;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                dst + offsets_of_aggregate_states[i],
                src + offsets_of_aggregate_states[i],
                arena);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);

        src = nullptr;
    });
    table_src.clearAndShrink();
}


void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(
    ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        AggregatedDataWithoutKey & res_data = res->without_key;
        AggregatedDataWithoutKey & current_data = non_empty_data[result_num]->without_key;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i], res->aggregates_pool);

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

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        if (!checkLimits(res->sizeWithoutOverflowRow(), no_more_keys))
            break;

        AggregatedDataVariants & current = *non_empty_data[result_num];

        if (!no_more_keys)
            mergeDataImpl<Method>(
                getDataVariant<Method>(*res).data,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);
        else if (res->without_key)
            mergeDataNoMoreKeysImpl<Method>(
                getDataVariant<Method>(*res).data,
                res->without_key,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);
        else
            mergeDataOnlyExistingKeysImpl<Method>(
                getDataVariant<Method>(*res).data,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);

        /// `current` will not destroy the states of aggregate functions in the destructor
        current.aggregator = nullptr;
    }
}

#define M(NAME) \
    template void NO_INLINE Aggregator::mergeSingleLevelDataImpl<decltype(AggregatedDataVariants::NAME)::element_type>( \
        ManyAggregatedDataVariants & non_empty_data) const;
    APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M

template <typename Method>
void NO_INLINE Aggregator::mergeBucketImpl(
    ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena, std::atomic<bool> * is_cancelled) const
{
    /// We merge all aggregation results to the first.
    AggregatedDataVariantsPtr & res = data[0];
    for (size_t result_num = 1, size = data.size(); result_num < size; ++result_num)
    {
        if (is_cancelled && is_cancelled->load(std::memory_order_seq_cst))
            return;

        AggregatedDataVariants & current = *data[result_num];

        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data.impls[bucket],
            getDataVariant<Method>(current).data.impls[bucket],
            arena);
    }
}


/** Combines aggregation states together, turns them into blocks, and outputs streams.
  * If the aggregation states are two-level, then it produces blocks strictly in order of 'bucket_num'.
  * (This is important for distributed processing.)
  * In doing so, it can handle different buckets in parallel, using up to `threads` threads.
  */
class MergingAndConvertingBlockInputStream : public IBlockInputStream
{
public:
    /** The input is a set of non-empty sets of partially aggregated data,
      *  which are all either single-level, or are two-level.
      */
    MergingAndConvertingBlockInputStream(const Aggregator & aggregator_, ManyAggregatedDataVariants & data_, bool final_, size_t threads_)
        : aggregator(aggregator_), data(data_), final(final_), threads(threads_)
    {
        /// At least we need one arena in first data item per thread
        if (!data.empty() && threads > data[0]->aggregates_pools.size())
        {
            Arenas & first_pool = data[0]->aggregates_pools;
            for (size_t j = first_pool.size(); j < threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }
    }

    String getName() const override { return "MergingAndConverting"; }

    Block getHeader() const override { return aggregator.getHeader(final); }

    ~MergingAndConvertingBlockInputStream() override
    {
        LOG_TRACE(&Poco::Logger::get(__PRETTY_FUNCTION__), "Waiting for threads to finish");

        /// We need to wait for threads to finish before destructor of 'parallel_merge_data',
        ///  because the threads access 'parallel_merge_data'.
        if (parallel_merge_data)
            parallel_merge_data->pool.wait();
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
                return aggregator.prepareBlockAndFillWithoutKey(
                    *first, final, first->type != AggregatedDataVariants::Type::without_key);
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
            if (false) {} // NOLINT
            APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
        #undef M
            else
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

            return aggregator.prepareBlockAndFillSingleLevel(*first, final);
        }
        else
        {
            if (!parallel_merge_data)
            {
                parallel_merge_data = std::make_unique<ParallelMergeData>(threads);
                for (size_t i = 0; i < threads; ++i)
                    scheduleThreadForNextBucket();
            }

            Block res;

            while (true)
            {
                std::unique_lock lock(parallel_merge_data->mutex);

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
        std::map<Int32, Block> ready_blocks;
        std::exception_ptr exception;
        std::mutex mutex;
        std::condition_variable condvar;
        ThreadPool pool;

        explicit ParallelMergeData(size_t threads_) : pool(threads_) {}
    };

    std::unique_ptr<ParallelMergeData> parallel_merge_data;

    void scheduleThreadForNextBucket()
    {
        ++max_scheduled_bucket_num;
        if (max_scheduled_bucket_num >= NUM_BUCKETS)
            return;

        parallel_merge_data->pool.scheduleOrThrowOnError(
            [this, max_scheduled_bucket_num = max_scheduled_bucket_num, group = CurrentThread::getGroup()]
            { return thread(max_scheduled_bucket_num, group); });
    }

    void thread(Int32 bucket_num, ThreadGroupStatusPtr thread_group)
    {
        try
        {
            setThreadName("MergingAggregtd");
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

            /// TODO: add no_more_keys support maybe

            auto & merged_data = *data[0];
            auto method = merged_data.type;
            Block block;

            /// Select Arena to avoid race conditions
            size_t thread_number = static_cast<size_t>(bucket_num) % threads;
            Arena * arena = merged_data.aggregates_pools.at(thread_number).get();

            if (false) {} // NOLINT
        #define M(NAME) \
            else if (method == AggregatedDataVariants::Type::NAME) \
            { \
                aggregator.mergeBucketImpl<decltype(merged_data.NAME)::element_type>(data, bucket_num, arena); \
                block = aggregator.convertOneBucketToBlock(merged_data, *merged_data.NAME, final, bucket_num); \
            }

            APPLY_FOR_VARIANTS_TWO_LEVEL(M)
        #undef M

            std::lock_guard lock(parallel_merge_data->mutex);
            parallel_merge_data->ready_blocks[bucket_num] = std::move(block);
        }
        catch (...)
        {
            std::lock_guard lock(parallel_merge_data->mutex);
            if (!parallel_merge_data->exception)
                parallel_merge_data->exception = std::current_exception();
        }

        parallel_merge_data->condvar.notify_all();
    }
};

ManyAggregatedDataVariants Aggregator::prepareVariantsToMerge(ManyAggregatedDataVariants & data_variants) const
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
        return {};

    if (non_empty_data.size() > 1)
    {
        /// Sort the states in descending order so that the merge is more efficient (since all states are merged into the first).
        std::sort(non_empty_data.begin(), non_empty_data.end(),
            [](const AggregatedDataVariantsPtr & lhs, const AggregatedDataVariantsPtr & rhs)
            {
                return lhs->sizeWithoutOverflowRow() > rhs->sizeWithoutOverflowRow();
            });
    }

    /// If at least one of the options is two-level, then convert all the options into two-level ones, if there are not such.
    /// Note - perhaps it would be more optimal not to convert single-level versions before the merge, but merge them separately, at the end.

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

        /** Elements from the remaining sets can be moved to the first data set.
          * Therefore, it must own all the arenas of all other sets.
          */
        first->aggregates_pools.insert(first->aggregates_pools.end(),
            non_empty_data[i]->aggregates_pools.begin(), non_empty_data[i]->aggregates_pools.end());
    }

    return non_empty_data;
}

std::unique_ptr<IBlockInputStream> Aggregator::mergeAndConvertToBlocks(
    ManyAggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    ManyAggregatedDataVariants non_empty_data = prepareVariantsToMerge(data_variants);

    if (non_empty_data.empty())
        return std::make_unique<NullBlockInputStream>(getHeader(final));

    return std::make_unique<MergingAndConvertingBlockInputStream>(*this, non_empty_data, final, max_threads);
}


template <bool no_more_keys, typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImplCase(
    Block & block,
    Arena * aggregates_pool,
    Method & method [[maybe_unused]],
    Table & data,
    AggregateDataPtr overflow_row) const
{
    ColumnRawPtrs key_columns(params.keys_size);
    AggregateColumnsConstData aggregate_columns(params.aggregates_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        aggregate_columns[i] = &typeid_cast<const ColumnAggregateFunction &>(*block.getByName(aggregate_column_name).column).getData();
    }

    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    /// For all rows.
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        if (!no_more_keys)
        {
            auto emplace_result = state.emplaceKey(data, i, *aggregates_pool);
            if (emplace_result.isInserted())
            {
                emplace_result.setMapped(nullptr);

                aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(aggregate_data);

                emplace_result.setMapped(aggregate_data);
            }
            else
                aggregate_data = emplace_result.getMapped();
        }
        else
        {
            auto find_result = state.findKey(data, i, *aggregates_pool);
            if (find_result.isFound())
                aggregate_data = find_result.getMapped();
        }

        /// aggregate_date == nullptr means that the new key did not fit in the hash table because of no_more_keys.

        /// If the key does not fit, and the data does not need to be aggregated into a separate row, then there's nothing to do.
        if (!aggregate_data && !overflow_row)
            continue;

        AggregateDataPtr value = aggregate_data ? aggregate_data : overflow_row;

        /// Merge state of aggregate functions.
        for (size_t j = 0; j < params.aggregates_size; ++j)
            aggregate_functions[j]->merge(
                value + offsets_of_aggregate_states[j],
                (*aggregate_columns[j])[i],
                aggregates_pool);
    }

    /// Early release memory.
    block.clear();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(
    Block & block,
    Arena * aggregates_pool,
    Method & method,
    Table & data,
    AggregateDataPtr overflow_row,
    bool no_more_keys) const
{
    if (!no_more_keys)
        mergeStreamsImplCase<false>(block, aggregates_pool, method, data, overflow_row);
    else
        mergeStreamsImplCase<true>(block, aggregates_pool, method, data, overflow_row);
}


void NO_INLINE Aggregator::mergeWithoutKeyStreamsImpl(
    Block & block,
    AggregatedDataVariants & result) const
{
    AggregateColumnsConstData aggregate_columns(params.aggregates_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        aggregate_columns[i] = &typeid_cast<const ColumnAggregateFunction &>(*block.getByName(aggregate_column_name).column).getData();
    }

    AggregatedDataWithoutKey & res = result.without_key;
    if (!res)
    {
        AggregateDataPtr place = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        res = place;
    }

    /// Adding Values
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0], result.aggregates_pool);

    /// Early release memory.
    block.clear();
}


void Aggregator::mergeStream(const BlockInputStreamPtr & stream, AggregatedDataVariants & result, size_t max_threads)
{
    if (isCancelled())
        return;

    /** If the remote servers used a two-level aggregation method,
      *  then blocks will contain information about the number of the bucket.
      * Then the calculations can be parallelized by buckets.
      * We decompose the blocks to the bucket numbers indicated in them.
      */
    BucketToBlocks bucket_to_blocks;

    /// Read all the data.
    LOG_TRACE(log, "Reading blocks of partially aggregated data.");

    size_t total_input_rows = 0;
    size_t total_input_blocks = 0;
    while (Block block = stream->read())
    {
        if (isCancelled())
            return;

        total_input_rows += block.rows();
        ++total_input_blocks;
        bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(block));
    }

    LOG_TRACE(log, "Read {} blocks of partially aggregated data, total {} rows.", total_input_blocks, total_input_rows);

    mergeBlocks(bucket_to_blocks, result, max_threads);
}

void Aggregator::mergeBlocks(BucketToBlocks bucket_to_blocks, AggregatedDataVariants & result, size_t max_threads)
{
    if (bucket_to_blocks.empty())
        return;

    UInt64 total_input_rows = 0;
    for (auto & bucket : bucket_to_blocks)
        for (auto & block : bucket.second)
            total_input_rows += block.rows();

    /** `minus one` means the absence of information about the bucket
      * - in the case of single-level aggregation, as well as for blocks with "overflowing" values.
      * If there is at least one block with a bucket number greater or equal than zero, then there was a two-level aggregation.
      */
    auto max_bucket = bucket_to_blocks.rbegin()->first;
    bool has_two_level = max_bucket >= 0;

    if (has_two_level)
    {
    #define M(NAME) \
        if (method_chosen == AggregatedDataVariants::Type::NAME) \
            method_chosen = AggregatedDataVariants::Type::NAME ## _two_level;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M
    }

    if (isCancelled())
        return;

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(method_chosen);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    bool has_blocks_with_unknown_bucket = bucket_to_blocks.count(-1);

    /// First, parallel the merge for the individual buckets. Then we continue merge the data not allocated to the buckets.
    if (has_two_level)
    {
        /** In this case, no_more_keys is not supported due to the fact that
          *  from different threads it is difficult to update the general state for "other" keys (overflows).
          * That is, the keys in the end can be significantly larger than max_rows_to_group_by.
          */

        LOG_TRACE(log, "Merging partially aggregated two-level data.");

        auto merge_bucket = [&bucket_to_blocks, &result, this](Int32 bucket, Arena * aggregates_pool, ThreadGroupStatusPtr thread_group)
        {
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);

            for (Block & block : bucket_to_blocks[bucket])
            {
                if (isCancelled())
                    return;

            #define M(NAME) \
                else if (result.type == AggregatedDataVariants::Type::NAME) \
                    mergeStreamsImpl(block, aggregates_pool, *result.NAME, result.NAME->data.impls[bucket], nullptr, false);

                if (false) {} // NOLINT
                    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
            #undef M
                else
                    throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
            }
        };

        std::unique_ptr<ThreadPool> thread_pool;
        if (max_threads > 1 && total_input_rows > 100000)    /// TODO Make a custom threshold.
            thread_pool = std::make_unique<ThreadPool>(max_threads);

        for (const auto & bucket_blocks : bucket_to_blocks)
        {
            const auto bucket = bucket_blocks.first;

            if (bucket == -1)
                continue;

            result.aggregates_pools.push_back(std::make_shared<Arena>());
            Arena * aggregates_pool = result.aggregates_pools.back().get();

            auto task = [group = CurrentThread::getGroup(), bucket, &merge_bucket, aggregates_pool]{ return merge_bucket(bucket, aggregates_pool, group); };

            if (thread_pool)
                thread_pool->scheduleOrThrowOnError(task);
            else
                task();
        }

        if (thread_pool)
            thread_pool->wait();

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
                mergeStreamsImpl(block, result.aggregates_pool, *result.NAME, result.NAME->data, result.without_key, no_more_keys);

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

    auto bucket_num = blocks.front().info.bucket_num;
    bool is_overflows = blocks.front().info.is_overflows;

    LOG_TRACE(log, "Merging partially aggregated blocks (bucket = {}).", bucket_num);
    Stopwatch watch;

    /** If possible, change 'method' to some_hash64. Otherwise, leave as is.
      * Better hash function is needed because during external aggregation,
      *  we may merge partitions of data with total number of keys far greater than 4 billion.
      */
    auto merge_method = method_chosen;

#define APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M) \
        M(key64)            \
        M(key_string)       \
        M(key_fixed_string) \
        M(keys128)          \
        M(keys256)          \
        M(serialized)       \

#define M(NAME) \
    if (merge_method == AggregatedDataVariants::Type::NAME) \
        merge_method = AggregatedDataVariants::Type::NAME ## _hash64; \

    APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M)
#undef M

#undef APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION

    /// Temporary data for aggregation.
    AggregatedDataVariants result;

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(merge_method);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    for (Block & block : blocks)
    {
        if (isCancelled())
            return {};

        if (bucket_num >= 0 && block.info.bucket_num != bucket_num)
            bucket_num = -1;

        if (result.type == AggregatedDataVariants::Type::without_key || is_overflows)
            mergeWithoutKeyStreamsImpl(block, result);

    #define M(NAME, IS_TWO_LEVEL) \
        else if (result.type == AggregatedDataVariants::Type::NAME) \
            mergeStreamsImpl(block, result.aggregates_pool, *result.NAME, result.NAME->data, nullptr, false);

        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
        else if (result.type != AggregatedDataVariants::Type::without_key)
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    if (isCancelled())
        return {};

    Block block;
    if (result.type == AggregatedDataVariants::Type::without_key || is_overflows)
        block = prepareBlockAndFillWithoutKey(result, final, is_overflows);
    else
        block = prepareBlockAndFillSingleLevel(result, final);
    /// NOTE: two-level data is not possible here - chooseAggregationMethod chooses only among single-level methods.

    if (!final)
    {
        /// Pass ownership of aggregate function states from result to ColumnAggregateFunction objects in the resulting block.
        result.aggregator = nullptr;
    }

    size_t rows = block.rows();
    size_t bytes = block.bytes();
    double elapsed_seconds = watch.elapsedSeconds();
    LOG_TRACE(log, "Merged partially aggregated blocks. {} rows, {}. in {} sec. ({} rows/sec., {}/sec.)",
        rows, ReadableSize(bytes),
        elapsed_seconds, rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    if (isCancelled())
        return {};

    block.info.bucket_num = bucket_num;
    return block;
}


template <typename Method>
void NO_INLINE Aggregator::convertBlockToTwoLevelImpl(
    Method & method,
    Arena * pool,
    ColumnRawPtrs & key_columns,
    const Block & source,
    std::vector<Block> & destinations) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    size_t rows = source.rows();
    size_t columns = source.columns();

    /// Create a 'selector' that will contain bucket index for every row. It will be used to scatter rows to buckets.
    IColumn::Selector selector(rows);

    /// For every row.
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (Method::low_cardinality_optimization)
        {
            if (state.isNullAt(i))
            {
                selector[i] = 0;
                continue;
            }
        }

        /// Calculate bucket number from row hash.
        auto hash = state.getHash(method.data, i, *pool);
        auto bucket = method.data.getBucketFromHash(hash);

        selector[i] = bucket;
    }

    size_t num_buckets = destinations.size();

    for (size_t column_idx = 0; column_idx < columns; ++column_idx)
    {
        const ColumnWithTypeAndName & src_col = source.getByPosition(column_idx);
        MutableColumns scattered_columns = src_col.column->scatter(num_buckets, selector);

        for (size_t bucket = 0, size = num_buckets; bucket < size; ++bucket)
        {
            if (!scattered_columns[bucket]->empty())
            {
                Block & dst = destinations[bucket];
                dst.info.bucket_num = bucket;
                dst.insert({std::move(scattered_columns[bucket]), src_col.type, src_col.name});
            }

            /** Inserted columns of type ColumnAggregateFunction will own states of aggregate functions
              *  by holding shared_ptr to source column. See ColumnAggregateFunction.h
              */
        }
    }
}


std::vector<Block> Aggregator::convertBlockToTwoLevel(const Block & block)
{
    if (!block)
        return {};

    AggregatedDataVariants data;

    ColumnRawPtrs key_columns(params.keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    AggregatedDataVariants::Type type = method_chosen;
    data.keys_size = params.keys_size;
    data.key_sizes = key_sizes;

#define M(NAME) \
    else if (type == AggregatedDataVariants::Type::NAME) \
        type = AggregatedDataVariants::Type::NAME ## _two_level;

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    data.init(type);

    size_t num_buckets = 0;

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        num_buckets = data.NAME->data.NUM_BUCKETS;

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    std::vector<Block> splitted_blocks(num_buckets);

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        convertBlockToTwoLevelImpl(*data.NAME, data.aggregates_pool, \
            key_columns, block, splitted_blocks);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    return splitted_blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::destroyImpl(Table & table) const
{
    table.forEachMapped([&](AggregateDataPtr & data)
    {
        /** If an exception (usually a lack of memory, the MemoryTracker throws) arose
          *  after inserting the key into a hash table, but before creating all states of aggregate functions,
          *  then data will be equal nullptr.
          */
        if (nullptr == data)
            return;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);

        data = nullptr;
    });
}


void Aggregator::destroyWithoutKey(AggregatedDataVariants & result) const
{
    AggregatedDataWithoutKey & res_data = result.without_key;

    if (nullptr != res_data)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);

        res_data = nullptr;
    }
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
    if (result.empty())
        return;

    LOG_TRACE(log, "Destroying aggregate states");

    /// In what data structure is the data aggregated?
    if (result.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
        destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        destroyImpl<decltype(result.NAME)::element_type>(result.NAME->data);

    if (false) {} // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    else if (result.type != AggregatedDataVariants::Type::without_key)
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


void Aggregator::setCancellationHook(const CancellationHook cancellation_hook)
{
    isCancelled = cancellation_hook;
}


}
