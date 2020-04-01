#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/MergedData.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/AlignedBuffer.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Core/Row.h>

namespace DB
{

namespace detail
{
    /// Stores aggregation function, state, and columns to be used as function arguments.
    struct AggregateDescription
    {
        /// An aggregate function 'sumWithOverflow' or 'sumMapWithOverflow' for summing.
        AggregateFunctionPtr function;
        IAggregateFunction::AddFunc add_function = nullptr;
        std::vector<size_t> column_numbers;
        IColumn * merged_column = nullptr;
        AlignedBuffer state;
        bool created = false;

        /// In case when column has type AggregateFunction: use the aggregate function from itself instead of 'function' above.
        bool is_agg_func_type = false;

        void init(const char * function_name, const DataTypes & argument_types)
        {
            function = AggregateFunctionFactory::instance().get(function_name, argument_types);
            add_function = function->getAddressOfAddFunction();
            state.reset(function->sizeOfData(), function->alignOfData());
        }

        void createState()
        {
            if (created)
                return;
            if (is_agg_func_type)
                merged_column->insertDefault();
            else
                function->create(state.data());
            created = true;
        }

        void destroyState()
        {
            if (!created)
                return;
            if (!is_agg_func_type)
                function->destroy(state.data());
            created = false;
        }

        /// Explicitly destroy aggregation state if the stream is terminated
        ~AggregateDescription()
        {
            destroyState();
        }

        AggregateDescription() = default;
        AggregateDescription(AggregateDescription &&) = default;
        AggregateDescription(const AggregateDescription &) = delete;
    };

    /// Specialization for SummingSortedTransform. Inserts only data for non-aggregated columns.
    struct SummingMergedData : public MergedData
    {
    public:
        using MergedData::MergedData;

        void insertRow(const Row & row, const ColumnNumbers & column_numbers)
        {
            for (auto column_number  :column_numbers)
                columns[column_number]->insert(row[column_number]);

            ++total_merged_rows;
            ++merged_rows;
            /// TODO: sum_blocks_granularity += block_size;
        }

        /// Initialize aggregate descriptions with columns.
        void initAggregateDescription(std::vector<AggregateDescription> & columns_to_aggregate)
        {
            size_t num_columns = columns_to_aggregate.size();
            for (size_t column_number = 0; column_number < num_columns; ++column_number)
                columns_to_aggregate[column_number].merged_column = columns[column_number].get();
        }
    };

    struct RowRef
    {
        ColumnRawPtrs * sort_columns = nullptr; /// Point to sort_columns from SortCursor or last_chunk_sort_columns.
        UInt64 row_num = 0;

        bool empty() const { return sort_columns == nullptr; }
        void reset() { sort_columns = nullptr; }

        void set(SortCursor & cursor)
        {
            sort_columns = &cursor.impl->sort_columns;
            row_num = cursor.impl->pos;
        }

        bool hasEqualSortColumnsWith(const RowRef & other)
        {
            auto size = sort_columns->size();
            for (size_t col_number = 0; col_number < size; ++col_number)
            {
                auto & cur_column = (*sort_columns)[col_number];
                auto & other_column = (*other.sort_columns)[col_number];

                if (0 != cur_column->compareAt(row_num, other.row_num, *other_column, 1))
                    return false;
            }

            return true;
        }
    };
}

class SummingSortedTransform : public IMergingTransform
{
public:

    SummingSortedTransform(
        size_t num_inputs, const Block & header,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        size_t max_block_size);

    /// Stores numbers of key-columns and value-columns.
    struct MapDescription
    {
        std::vector<size_t> key_col_nums;
        std::vector<size_t> val_col_nums;
    };

    struct ColumnsDefinition
    {
        /// Columns with which values should be summed.
        ColumnNumbers column_numbers_not_to_aggregate;
        /// Columns which should be aggregated.
        std::vector<detail::AggregateDescription> columns_to_aggregate;
        /// Mapping for nested columns.
        std::vector<MapDescription> maps_to_sum;

        size_t getNumColumns() const { return column_numbers_not_to_aggregate.size() + columns_to_aggregate.size(); }
    };

    String getName() const override { return "SummingSortedTransform"; }
    void work() override;

protected:
    void initializeInputs() override;
    void consume(Chunk chunk, size_t input_number) override;

private:
    Row current_row;
    bool current_row_is_zero = true;    /// Are all summed columns zero (or empty)? It is updated incrementally.

    ColumnsDefinition columns_definition;
    detail::SummingMergedData merged_data;

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

    void merge();
    void updateCursor(Chunk chunk, size_t source_num);
    void addRow(SortCursor & cursor);
    void insertCurrentRowIfNeeded();
};

}
