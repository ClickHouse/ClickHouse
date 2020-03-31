#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/MergedData.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/AlignedBuffer.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

namespace DB
{



class SummingSortedTransform : public IMergingTransform
{
public:

    SummingSortedTransform(
        size_t num_inputs, const Block & header,
        SortDescription description,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        size_t max_block_size);

    /// Stores aggregation function, state, and columns to be used as function arguments
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

    struct SummingMergedData : public MergedData
    {
    public:

    };

    /// Stores numbers of key-columns and value-columns.
    struct MapDescription
    {
        std::vector<size_t> key_col_nums;
        std::vector<size_t> val_col_nums;
    };

private:
    /// Columns with which values should be summed.
    ColumnNumbers column_numbers_not_to_aggregate;

    std::vector<AggregateDescription> columns_to_aggregate;
    std::vector<MapDescription> maps_to_sum;
};

}
