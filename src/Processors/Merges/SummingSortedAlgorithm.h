#pragma once

#include <Processors/Merges/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/MergedData.h>
#include <Core/Row.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class SummingSortedAlgorithm : public IMergingAlgorithmWithDelayedChunk
{
public:
    SummingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        size_t max_block_size);

    void initialize(Chunks chunks) override;
    void consume(Chunk chunk, size_t source_num) override;
    Status merge() override;

    struct AggregateDescription;

    /// Stores numbers of key-columns and value-columns.
    struct MapDescription
    {
        std::vector<size_t> key_col_nums;
        std::vector<size_t> val_col_nums;
    };

    /// This structure define columns into one of three types:
    /// * columns which values not needed to be aggregated
    /// * aggregate functions and columns which needed to be summed
    /// * mapping for nested columns
    struct ColumnsDefinition
    {
        /// Columns with which values should not be aggregated.
        ColumnNumbers column_numbers_not_to_aggregate;
        /// Columns which should be aggregated.
        std::vector<AggregateDescription> columns_to_aggregate;
        /// Mapping for nested columns.
        std::vector<MapDescription> maps_to_sum;

        size_t getNumColumns() const { return column_numbers_not_to_aggregate.size() + columns_to_aggregate.size(); }
    };

    /// Specialization for SummingSortedTransform. Inserts only data for non-aggregated columns.
    class SummingMergedData : public MergedData
    {
    private:
        using MergedData::pull;

    public:
        using MergedData::MergedData;

        SummingMergedData(MutableColumns columns_, UInt64 max_block_size_, ColumnsDefinition & def_)
            : MergedData(std::move(columns_), false, max_block_size_)
            , def(def_)
        {
        }

        void insertRow(const Row & row, const ColumnNumbers & column_numbers)
        {
            size_t next_column = columns.size() - column_numbers.size();
            for (auto column_number : column_numbers)
            {
                columns[next_column]->insert(row[column_number]);
                ++next_column;
            }

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

        Chunk pull(size_t num_result_columns);

    private:
        ColumnsDefinition & def;
    };

private:
    Row current_row;
    bool current_row_is_zero = true;    /// Are all summed columns zero (or empty)? It is updated incrementally.

    ColumnsDefinition columns_definition;
    SummingMergedData merged_data;

    Names column_names;

    void addRow(SortCursor & cursor);
    void insertCurrentRowIfNeeded();

public:
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
};

}
