#include <Processors/Merges/Algorithms/AggregatingSortedAlgorithm.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AggregatingSortedAlgorithm::ColumnsDefinition::ColumnsDefinition() = default;
AggregatingSortedAlgorithm::ColumnsDefinition::ColumnsDefinition(ColumnsDefinition &&) noexcept = default;
AggregatingSortedAlgorithm::ColumnsDefinition::~ColumnsDefinition() = default;

static AggregatingSortedAlgorithm::ColumnsDefinition defineColumns(
    const Block & header, const SortDescription & description)
{
    AggregatingSortedAlgorithm::ColumnsDefinition def = {};
    size_t num_columns = header.columns();

    /// Fill in the column numbers that need to be aggregated.
    for (size_t i = 0; i < num_columns; ++i)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        /// We leave only states of aggregate functions.
        if (!dynamic_cast<const DataTypeAggregateFunction *>(column.type.get())
            && !dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
        {
            def.column_numbers_not_to_aggregate.push_back(i);
            continue;
        }

        /// Included into PK?
        auto it = description.begin();
        for (; it != description.end(); ++it)
            if (it->column_name == column.name)
                break;

        if (it != description.end())
        {
            def.column_numbers_not_to_aggregate.push_back(i);
            continue;
        }

        if (const auto * simple = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
        {
            auto type = recursiveRemoveLowCardinality(column.type);
            if (type.get() == column.type.get())
                type = nullptr;

            // simple aggregate function
            AggregatingSortedAlgorithm::SimpleAggregateDescription desc(simple->getFunction(), i, type, column.type);
            if (desc.function->allocatesMemoryInArena())
                def.allocates_memory_in_arena = true;

            def.columns_to_simple_aggregate.emplace_back(std::move(desc));
        }
        else
        {
            // standard aggregate function
            def.columns_to_aggregate.emplace_back(i);
        }
    }

    return def;
}

/// Remove constants and LowCardinality for SimpleAggregateFunction
static void preprocessChunk(Chunk & chunk, const AggregatingSortedAlgorithm::ColumnsDefinition & def)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (const auto & desc : def.columns_to_simple_aggregate)
        if (desc.nested_type)
            columns[desc.column_number] = recursiveRemoveLowCardinality(columns[desc.column_number]);

    chunk.setColumns(std::move(columns), num_rows);
}

/// Return back LowCardinality for SimpleAggregateFunction
static void postprocessChunk(Chunk & chunk, const AggregatingSortedAlgorithm::ColumnsDefinition & def)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (const auto & desc : def.columns_to_simple_aggregate)
    {
        if (desc.nested_type)
        {
            const auto & from_type = desc.nested_type;
            const auto & to_type = desc.real_type;
            columns[desc.column_number] = recursiveLowCardinalityTypeConversion(columns[desc.column_number], from_type, to_type);
        }
    }

    chunk.setColumns(std::move(columns), num_rows);
}


AggregatingSortedAlgorithm::SimpleAggregateDescription::SimpleAggregateDescription(
    AggregateFunctionPtr function_, size_t column_number_,
    DataTypePtr nested_type_, DataTypePtr real_type_)
    : function(std::move(function_)), column_number(column_number_)
    , nested_type(std::move(nested_type_)), real_type(std::move(real_type_))
{
    add_function = function->getAddressOfAddFunction();
    state.reset(function->sizeOfData(), function->alignOfData());
}

void AggregatingSortedAlgorithm::SimpleAggregateDescription::createState()
{
    if (created)
        return;
    function->create(state.data());
    created = true;
}

void AggregatingSortedAlgorithm::SimpleAggregateDescription::destroyState()
{
    if (!created)
        return;
    function->destroy(state.data());
    created = false;
}

/// Explicitly destroy aggregation state if the stream is terminated
AggregatingSortedAlgorithm::SimpleAggregateDescription::~SimpleAggregateDescription()
{
    destroyState();
}


AggregatingSortedAlgorithm::AggregatingMergedData::AggregatingMergedData(
    UInt64 max_block_size_rows_,
    UInt64 max_block_size_bytes_,
    ColumnsDefinition & def_)
    : MergedData(false, max_block_size_rows_, max_block_size_bytes_), def(def_)
{
}

void AggregatingSortedAlgorithm::AggregatingMergedData::initialize(const DB::Block & header, const IMergingAlgorithm::Inputs & inputs)
{
    MergedData::initialize(header, inputs);

    for (const auto & desc : def.columns_to_simple_aggregate)
    {
        const auto & type = desc.nested_type ? desc.nested_type
                                             : desc.real_type;
        columns[desc.column_number] = type->createColumn();
    }

    initAggregateDescription();

    /// Just to make startGroup() simpler.
    if (def.allocates_memory_in_arena)
    {
        arena = std::make_unique<Arena>();
        arena_size = arena->allocatedBytes();
    }
}

void AggregatingSortedAlgorithm::AggregatingMergedData::startGroup(const ColumnRawPtrs & raw_columns, size_t row)
{
    /// We will write the data for the group. We copy the values of ordinary columns.
    for (auto column_number : def.column_numbers_not_to_aggregate)
        columns[column_number]->insertFrom(*raw_columns[column_number], row);

    /// Add the empty aggregation state to the aggregate columns. The state will be updated in the `addRow` function.
    for (auto & column_to_aggregate : def.columns_to_aggregate)
        column_to_aggregate.column->insertDefault();

    /// Reset simple aggregation states for next row
    for (auto & desc : def.columns_to_simple_aggregate)
        desc.createState();

    /// Frequent Arena creation may be too costly, because we have to increment the atomic
    /// ProfileEvents counters when creating the first Chunk -- e.g. SELECT with
    /// SimpleAggregateFunction(String) in PK and lots of groups may produce ~1.5M of
    /// ArenaAllocChunks atomic increments, while LOCK is too costly for CPU
    /// (~10% overhead here).
    /// To avoid this, reset arena if and only if:
    /// - arena is required (i.e. SimpleAggregateFunction(any, String) in PK),
    /// - arena was used in the previous groups.
    if (def.allocates_memory_in_arena && arena->allocatedBytes() > arena_size)
    {
        arena = std::make_unique<Arena>();
        arena_size = arena->allocatedBytes();
    }

    is_group_started = true;
}

void AggregatingSortedAlgorithm::AggregatingMergedData::finishGroup()
{
    /// Write the simple aggregation result for the current group.
    for (auto & desc : def.columns_to_simple_aggregate)
    {
        desc.function->insertResultInto(desc.state.data(), *desc.column, arena.get());
        desc.destroyState();
    }

    is_group_started = false;
    ++total_merged_rows;
    ++merged_rows;
    /// TODO: sum_blocks_granularity += block_size;
}

void AggregatingSortedAlgorithm::AggregatingMergedData::addRow(SortCursor & cursor)
{
    if (!is_group_started)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't add a row to the group because it was not started.");

    for (auto & desc : def.columns_to_aggregate)
        desc.column->insertMergeFrom(*cursor->all_columns[desc.column_number], cursor->getRow());

    for (auto & desc : def.columns_to_simple_aggregate)
    {
        auto & col = cursor->all_columns[desc.column_number];
        desc.add_function(desc.function.get(), desc.state.data(), &col, cursor->getRow(), arena.get());
    }
}

Chunk AggregatingSortedAlgorithm::AggregatingMergedData::pull()
{
    if (is_group_started)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't pull chunk because group was not finished.");

    auto chunk = MergedData::pull();
    postprocessChunk(chunk, def);

    initAggregateDescription();

    return chunk;
}

void AggregatingSortedAlgorithm::AggregatingMergedData::initAggregateDescription()
{
    for (auto & desc : def.columns_to_simple_aggregate)
        desc.column = columns[desc.column_number].get();

    for (auto & desc : def.columns_to_aggregate)
        desc.column = typeid_cast<ColumnAggregateFunction *>(columns[desc.column_number].get());
}


AggregatingSortedAlgorithm::AggregatingSortedAlgorithm(
    const Block & header_,
    size_t num_inputs,
    SortDescription description_,
    size_t max_block_size_rows_,
    size_t max_block_size_bytes_)
    : IMergingAlgorithmWithDelayedChunk(header_, num_inputs, description_)
    , columns_definition(defineColumns(header_, description_))
    , merged_data(max_block_size_rows_, max_block_size_bytes_, columns_definition)
{
}

void AggregatingSortedAlgorithm::initialize(Inputs inputs)
{
    removeConstAndSparse(inputs);
    merged_data.initialize(header, inputs);

    for (auto & input : inputs)
        if (input.chunk)
            preprocessChunk(input.chunk, columns_definition);

    initializeQueue(std::move(inputs));
}

void AggregatingSortedAlgorithm::consume(Input & input, size_t source_num)
{
    removeConstAndSparse(input);
    preprocessChunk(input.chunk, columns_definition);
    updateCursor(input, source_num);
}

IMergingAlgorithm::Status AggregatingSortedAlgorithm::merge()
{
    /// We take the rows in the correct order and put them in `merged_block`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        bool key_differs;
        SortCursor current = queue.current();

        if (current->isLast() && skipLastRowFor(current->order))
        {
            /// If we skip this row, it's not equals with any key we process.
            last_key.reset();
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        {
            detail::RowRef current_key;
            setRowRef(current_key, current);

            key_differs = last_key.empty() || rowsHaveDifferentSortColumns(last_key, current_key);

            last_key = current_key;
            last_chunk_sort_columns.clear();
        }

        if (key_differs)
        {
            if (merged_data.isGroupStarted())
                merged_data.finishGroup();

            /// if there are enough rows accumulated and the last one is calculated completely
            if (merged_data.hasEnoughRows())
            {
                last_key.reset();
                return Status(merged_data.pull());
            }

            merged_data.startGroup(current->all_columns, current->getRow());
        }

        merged_data.addRow(current);

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// Write the simple aggregation result for the previous group.
    if (merged_data.isGroupStarted())
        merged_data.finishGroup();

    last_chunk_sort_columns.clear();
    return Status(merged_data.pull(), true);
}

}
