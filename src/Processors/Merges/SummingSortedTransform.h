#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/SummingSortedAlgorithm.h>

namespace ProfileEvents
{
    extern const Event SummingSortedMilliseconds;
}

namespace DB
{

/// Implementation of IMergingTransform via SummingSortedAlgorithm.
class SummingSortedTransform final : public IMergingTransform<SummingSortedAlgorithm>
{
public:
    static constexpr auto sum_function_name = "sumWithOverflow";
    static constexpr auto sum_function_map_name = "sumMapWithOverflow";
    static constexpr bool remove_default_values = true;
    static constexpr bool aggregate_all_columns = false;

    /// See `SummingSortedAlgorithm::getAggregatedColumnNames`.
    static NameSet getAggregatedColumnNames(
        const Block & sample_header,
        const SortDescription & sort_description,
        const Names & column_names_to_sum,
        const Names & partition_and_sorting_required_columns,
        bool allow_tuple_element_aggregation)
    {
        return SummingSortedAlgorithm::getAggregatedColumnNames(
            sample_header,
            sort_description,
            column_names_to_sum,
            partition_and_sorting_required_columns,
            sum_function_name,
            sum_function_map_name,
            remove_default_values,
            aggregate_all_columns,
            allow_tuple_element_aggregation);
    }

    SummingSortedTransform(
        SharedHeader header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & partition_and_sorting_required_columns,
        const Names & partition_key_columns,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        std::optional<size_t> max_dynamic_subcolumns_,
        bool allow_tuple_element_aggregation
        )
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
            header,
            num_inputs,
            std::move(description_),
            partition_and_sorting_required_columns,
            partition_key_columns,
            max_block_size_rows,
            max_block_size_bytes,
            max_dynamic_subcolumns_,
            sum_function_name,
            sum_function_map_name,
            remove_default_values,
            aggregate_all_columns,
            allow_tuple_element_aggregation)
    {
    }

    String getName() const override { return "SummingSortedTransform"; }

    void onFinish() override
    {
        logMergedStats(ProfileEvents::SummingSortedMilliseconds, "Summed sorted", getLogger("SummingSortedTransform"));
    }
};

}
