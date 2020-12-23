#include <Processors/Merges/Algorithms/AggregatingNoSortedAlgorithm.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AggregatingNoSortedAlgorithm::ColumnsDefinition::ColumnsDefinition() = default;
AggregatingNoSortedAlgorithm::ColumnsDefinition::ColumnsDefinition(ColumnsDefinition &&) noexcept = default;
AggregatingNoSortedAlgorithm::ColumnsDefinition::~ColumnsDefinition() = default;

/// Stores information for aggregation of AggregateFunction columns
struct AggregatingNoSortedAlgorithm::AggregateDescription
{
    ColumnAggregateFunction * column = nullptr;
    const size_t column_number = 0; /// Position in header.

    AggregateDescription() = default;
    explicit AggregateDescription(size_t col_number) : column_number(col_number) {}
};

/// Stores information for aggregation of SimpleAggregateFunction columns
struct AggregatingNoSortedAlgorithm::SimpleAggregateDescription
{
    /// An aggregate function 'anyLast', 'sum'...
    AggregateFunctionPtr function;
    IAggregateFunction::AddFunc add_function = nullptr;

    size_t column_number = 0;
    IColumn * column = nullptr;

    /// For LowCardinality, convert is converted to nested type. nested_type is nullptr if no conversion needed.
    const DataTypePtr nested_type; /// Nested type for LowCardinality, if it is.
    const DataTypePtr real_type; /// Type in header.

    AlignedBuffer state;
    bool created = false;

    SimpleAggregateDescription(
            AggregateFunctionPtr function_, const size_t column_number_,
            DataTypePtr nested_type_, DataTypePtr real_type_)
            : function(std::move(function_)), column_number(column_number_)
            , nested_type(std::move(nested_type_)), real_type(std::move(real_type_))
    {
        add_function = function->getAddressOfAddFunction();
        state.reset(function->sizeOfData(), function->alignOfData());
    }

    void createState()
    {
        if (created)
            return;
        function->create(state.data());
        created = true;
    }

    void destroyState()
    {
        if (!created)
            return;
        function->destroy(state.data());
        created = false;
    }

    /// Explicitly destroy aggregation state if the stream is terminated
    ~SimpleAggregateDescription()
    {
        destroyState();
    }

    SimpleAggregateDescription() = default;
    SimpleAggregateDescription(SimpleAggregateDescription &&) = default;
    SimpleAggregateDescription(const SimpleAggregateDescription &) = delete;
};

static AggregatingNoSortedAlgorithm::ColumnsDefinition defineColumns(const Block & header)
{
    AggregatingNoSortedAlgorithm::ColumnsDefinition def = {};
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

        if (const auto * simple = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
        {
            auto type = recursiveRemoveLowCardinality(column.type);
            if (type.get() == column.type.get())
                type = nullptr;

            // simple aggregate function
            AggregatingNoSortedAlgorithm::SimpleAggregateDescription desc(simple->getFunction(), i, type, column.type);
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
static void preprocessChunk(Chunk & chunk, const AggregatingNoSortedAlgorithm::ColumnsDefinition & def)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    for (const auto & desc : def.columns_to_simple_aggregate)
        if (desc.nested_type)
            columns[desc.column_number] = recursiveRemoveLowCardinality(columns[desc.column_number]);

    chunk.setColumns(std::move(columns), num_rows);
}

/// Return back LowCardinality for SimpleAggregateFunction
static void postprocessChunk(Chunk & chunk, const AggregatingNoSortedAlgorithm::ColumnsDefinition & def)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (const auto & desc : def.columns_to_simple_aggregate)
    {
        if (desc.nested_type)
        {
            const auto & from_type = desc.nested_type;
            const auto & to_type = desc.real_type;
            columns[desc.column_number] = recursiveTypeConversion(columns[desc.column_number], from_type, to_type);
        }
    }

    chunk.setColumns(std::move(columns), num_rows);
}

static MutableColumns getMergedColumns(const Block & header, const AggregatingNoSortedAlgorithm::ColumnsDefinition & def)
{
    MutableColumns columns;
    columns.resize(header.columns());

    for (const auto & desc : def.columns_to_simple_aggregate)
    {
        const auto & type = desc.nested_type ? desc.nested_type
                                       : desc.real_type;
        columns[desc.column_number] = type->createColumn();
    }

    for (size_t i = 0; i < columns.size(); ++i)
        if (!columns[i])
            columns[i] = header.getByPosition(i).type->createColumn();

    return columns;
}

AggregatingNoSortedAlgorithm::AggregatingNoSortedAlgorithm(const Block & header)
    : columns_definition(defineColumns(header)),
      columns(getMergedColumns(header, columns_definition))
{
    for (auto & desc : columns_definition.columns_to_simple_aggregate)
        desc.column = columns[desc.column_number].get();

    for (auto & desc : columns_definition.columns_to_aggregate)
        desc.column = typeid_cast<ColumnAggregateFunction *>(columns[desc.column_number].get());

    /// Just to make startGroup() simpler.
    if (columns_definition.allocates_memory_in_arena)
    {
        arena = std::make_unique<Arena>();
        arena_size = arena->size();
    }

    /// Add the empty aggregation state to the aggregate columns. The state will be updated in the `addRow` function.
    for (auto & column_to_aggregate : columns_definition.columns_to_aggregate)
        column_to_aggregate.column->insertDefault();

    /// Reset simple aggregation states for next row
    for (auto & desc : columns_definition.columns_to_simple_aggregate)
        desc.createState();

    /// Frequent Arena creation may be too costly, because we have to increment the atomic
    /// ProfileEvents counters when creating the first Chunk -- e.g. SELECT with
    /// SimpleAggregateFunction(String) in PK and lots of groups may produce ~1.5M of
    /// ArenaAllocChunks atomic increments, while LOCK is too costly for CPU
    /// (~10% overhead here).
    /// To avoid this, reset arena if and only if:
    /// - arena is required (i.e. SimpleAggregateFunction(any, String) in PK),
    /// - arena was used in the previous groups.
    if (columns_definition.allocates_memory_in_arena && arena->size() > arena_size)
    {
        arena = std::make_unique<Arena>();
        arena_size = arena->size();
    }
}

void AggregatingNoSortedAlgorithm::initialize(Inputs inputs_)
{
    inputs = std::move(inputs_);
    for (auto & input : inputs)
        if (input.chunk)
            preprocessChunk(input.chunk, columns_definition);
}

IMergingAlgorithm::Status AggregatingNoSortedAlgorithm::merge()
{
    for (auto & input : inputs)
    {
        if (!input.chunk)
            continue;

        for (auto & desc : columns_definition.columns_to_aggregate)
            desc.column->insertMergeFrom(*input.chunk.getColumns()[desc.column_number], 0);

        for (auto & desc : columns_definition.columns_to_simple_aggregate)
        {
            const auto * col = &*input.chunk.getColumns()[desc.column_number];
            desc.add_function(desc.function.get(), desc.state.data(), &col, 0, arena.get());
        }
    }

    for (auto & desc : columns_definition.columns_to_simple_aggregate)
    {
        desc.function->insertResultInto(desc.state.data(), *desc.column, arena.get());
        desc.destroyState();
    }

    Chunk chunk(std::move(columns), 1);
    postprocessChunk(chunk, columns_definition);
    return IMergingAlgorithm::Status(std::move(chunk), true);
}

}
