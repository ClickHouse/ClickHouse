#pragma once

#include <Processors/Merges/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/MergedData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnAggregateFunction.h>

namespace DB
{

class AggregatingSortedAlgorithm final : public IMergingAlgorithmWithDelayedChunk
{
public:
    AggregatingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size);

    void initialize(Chunks chunks) override;
    void consume(Chunk chunk, size_t source_num) override;
    Status merge() override;

    struct SimpleAggregateDescription;

    struct ColumnsDefinition
    {
        struct AggregateDescription
        {
            ColumnAggregateFunction * column = nullptr;
            const size_t column_number = 0;

            AggregateDescription() = default;
            explicit AggregateDescription(size_t col_number) : column_number(col_number) {}
        };

        /// Columns with which numbers should not be aggregated.
        ColumnNumbers column_numbers_not_to_aggregate;
        std::vector<AggregateDescription> columns_to_aggregate;
        std::vector<SimpleAggregateDescription> columns_to_simple_aggregate;

        /// Does SimpleAggregateFunction allocates memory in arena?
        bool allocates_memory_in_arena = false;
    };

private:
    /// Specialization for AggregatingSortedAlgorithm.
    struct AggregatingMergedData : public MergedData
    {
    public:
        AggregatingMergedData(MutableColumns columns_, UInt64 max_block_size_, ColumnsDefinition & def)
            : MergedData(std::move(columns_), false, max_block_size_)
        {
            initAggregateDescription(def);
        }

        void initializeRow(const ColumnRawPtrs & raw_columns, size_t row, const ColumnNumbers & column_numbers)
        {
            for (auto column_number : column_numbers)
                columns[column_number]->insertFrom(*raw_columns[column_number], row);

            is_group_started = true;
        }

        bool isGroupStarted() const { return is_group_started; }

        void insertRow()
        {
            is_group_started = false;
            ++total_merged_rows;
            ++merged_rows;
            /// TODO: sum_blocks_granularity += block_size;
        }

        Chunk pull(ColumnsDefinition & def, const Block & header_)
        {
            auto chunk = pull();

            size_t num_rows = chunk.getNumRows();
            auto columns_ = chunk.detachColumns();

            for (auto & desc : def.columns_to_simple_aggregate)
            {
                if (desc.inner_type)
                {
                    auto & from_type = desc.inner_type;
                    auto & to_type = header_.getByPosition(desc.column_number).type;
                    columns_[desc.column_number] = recursiveTypeConversion(columns_[desc.column_number], from_type, to_type);
                }
            }

            chunk.setColumns(std::move(columns_), num_rows);
            initAggregateDescription(def);

            return chunk;
        }

    private:
        bool is_group_started = false;

        /// Initialize aggregate descriptions with columns.
        void initAggregateDescription(ColumnsDefinition & def)
        {
            for (auto & desc : def.columns_to_simple_aggregate)
                desc.column = columns[desc.column_number].get();

            for (auto & desc : def.columns_to_aggregate)
                desc.column = typeid_cast<ColumnAggregateFunction *>(columns[desc.column_number].get());
        }

        using MergedData::pull;
    };

    Block header;

    ColumnsDefinition columns_definition;
    AggregatingMergedData merged_data;

    /// Memory pool for SimpleAggregateFunction
    /// (only when allocates_memory_in_arena == true).
    std::unique_ptr<Arena> arena;

    void prepareChunk(Chunk & chunk) const;
    void addRow(SortCursor & cursor);
    void insertSimpleAggregationResult();

public:
    /// Stores information for aggregation of SimpleAggregateFunction columns
    struct SimpleAggregateDescription
    {
        /// An aggregate function 'anyLast', 'sum'...
        AggregateFunctionPtr function;
        IAggregateFunction::AddFunc add_function = nullptr;

        size_t column_number = 0;
        IColumn * column = nullptr;
        const DataTypePtr inner_type;

        AlignedBuffer state;
        bool created = false;

        SimpleAggregateDescription(AggregateFunctionPtr function_, const size_t column_number_, DataTypePtr type)
                : function(std::move(function_)), column_number(column_number_), inner_type(std::move(type))
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
};

}
