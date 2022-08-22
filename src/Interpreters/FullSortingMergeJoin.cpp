#include <Interpreters/FullSortingMergeJoin.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

namespace
{

SortDescription getSortDescription(const DataStream & data_stream, const Names & key_names)
{
    SortDescription sort_descr;
    if (data_stream.sort_mode >= DataStream::SortMode::Port)
    {
        std::unordered_map<String, SortColumnDescription> sort_columns_map;
        for (const auto & desc : data_stream.sort_description)
            sort_columns_map.emplace(desc.column_name, desc);

        /// Use prefix of current sorting as much as possible
        for (const auto & key_name : key_names)
        {
            if (auto desc = sort_columns_map.find(key_name); desc != sort_columns_map.end())
                sort_descr.emplace_back(desc->second);
            else
                break;
        }
    }
    return sort_descr;
}

}

void FullSortingMergeJoin::getSortDescriptions(
    const DataStream & left_data_stream, const Names & left_key_names,
    const DataStream & right_data_stream, const Names & right_key_names,
    SortDescription & left_sort_descr, SortDescription & right_sort_descr)
{
    assert(left_key_names.size() == right_key_names.size());

    left_sort_descr = getSortDescription(left_data_stream, left_key_names);
    right_sort_descr = getSortDescription(right_data_stream, right_key_names);

    /// Use common directions
    size_t n = std::max(left_sort_descr.size(), right_sort_descr.size());
    for (size_t pos = 0; pos < n; ++pos)
    {
        if (left_sort_descr.size() <= pos)
        {
            left_sort_descr.emplace_back(left_key_names[pos], right_sort_descr[pos].direction, right_sort_descr[pos].nulls_direction);
        }
        else if (right_sort_descr.size() <= pos)
        {
            right_sort_descr.emplace_back(right_key_names[pos], left_sort_descr[pos].direction, left_sort_descr[pos].nulls_direction);
        }
        else
        {
            /// If both are sorted by same prefix by in different directions, then we can use one of them and resort another one
            left_sort_descr[pos].direction = right_sort_descr[pos].direction;
            left_sort_descr[pos].nulls_direction = right_sort_descr[pos].nulls_direction;
        }
    }

    /// Not sorted by all keys, add rest of them

    for (size_t pos = left_sort_descr.size(); pos < left_key_names.size(); ++pos)
        left_sort_descr.emplace_back(left_key_names[pos]);

    for (size_t pos = right_sort_descr.size(); pos < right_key_names.size(); ++pos)
        right_sort_descr.emplace_back(right_key_names[pos]);
}


}
