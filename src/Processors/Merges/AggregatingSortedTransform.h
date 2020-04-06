#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/MergedData.h>
#include <Processors/Merges/RowRef.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Core/Row.h>

namespace DB
{

class ColumnAggregateFunction;

class AggregatingSortedTransform : public IMergingTransform
{
public:
    AggregatingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size);

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

    String getName() const override { return "AggregatingSortedTransform"; }
    void work() override;

protected:
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;

private:

    /// Specialization for SummingSortedTransform. Inserts only data for non-aggregated columns.
    struct AggregatingMergedData : public MergedData
    {
    public:
        using MergedData::MergedData;

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

        /// Initialize aggregate descriptions with columns.
        void initAggregateDescription(ColumnsDefinition & def)
        {
            for (auto & desc : def.columns_to_simple_aggregate)
                desc.column = columns[desc.column_number].get();

            for (auto & desc : def.columns_to_aggregate)
                desc.column = typeid_cast<ColumnAggregateFunction *>(columns[desc.column_number].get());
        }
    private:
        bool is_group_started = false;
    };

    ColumnsDefinition columns_definition;
    AggregatingMergedData merged_data;

    SortDescription description;

    /// Chunks currently being merged.
    std::vector<Chunk> source_chunks;
    SortCursorImpls cursors;

    /// In merging algorithm, we need to compare current sort key with the last one.
    /// So, sorting columns for last row needed to be stored.
    /// In order to do it, we extend lifetime of last chunk and it's sort columns (from corresponding sort cursor).
    Chunk last_chunk;
    ColumnRawPtrs last_chunk_sort_columns; /// Point to last_chunk if valid.

    detail::RowRef last_key;

    SortingHeap<SortCursor> queue;
    bool is_queue_initialized = false;

    /// Memory pool for SimpleAggregateFunction
    /// (only when allocates_memory_in_arena == true).
    std::unique_ptr<Arena> arena;

    void merge();
    void updateCursor(Chunk chunk, size_t source_num);
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
