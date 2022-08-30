#include <Interpreters/FullSortingMergeJoin.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

static std::unordered_map<String, size_t> getSortIndexes(const DataStream & data_stream, const NameSet & key_names)
{
    UNUSED(key_names);
    std::unordered_map<String, size_t> sort_indexes;
    if (data_stream.sort_mode >= DataStream::SortMode::Port)
    {
        for (size_t i = 0; i < data_stream.sort_description.size(); ++i)
        {
            const auto & desc = data_stream.sort_description[i];
            if (!desc.isDefaultDirection() || !key_names.contains(desc.column_name))
                break;
            sort_indexes[desc.column_name] = i;
        }
    }
    return sort_indexes;
}

static size_t getIdxFromNameMap(const std::unordered_map<String, size_t> & indexes, const String & name)
{
    auto it = indexes.find(name);
    /// If key not found, use the maximal possible value
    if (it == indexes.end())
        return indexes.size();
    return it->second;
}

/* Use existing sorting as much as possible, and still need to keep correspondence between keys.
 * A query can have keys in any order in ON or USING expression.
 *
 * Example:
 * `JOIN ON t1.a2 = t2.b3 AND t1.a1 = t2.b1 AND t1.a5 = t2.b4 AND t1.a1 = t2.b2 AND t1.a4 = t2.b3`
 * Assume we have streams sorted by `(a1, a2, a3, a4, a5)` and `(b1, b2, b3, b4)`
 * We want to put keys into the result in the same order.
 * So, the join keys would be `[<a1, b1>, <a1, b2>, <a2, b3>, <a4, b3>, <a5, b4>]`
 * It would use sort prefixes `(a1, a2)` and `(b1, b2, b3, b4)`.
 *
 * Only ascending and direction can be used (because the current implementation of merge join expects it).
 */
void FullSortingMergeJoin::deduceSortDescriptions(
    const DataStream & left_data_stream, const Names & left_key_names,
    const DataStream & right_data_stream, const Names & right_key_names,
    SortDescription & left_sort_descr, SortDescription & right_sort_descr)
{
    assert(left_key_names.size() == right_key_names.size());
    size_t keys_size = left_key_names.size();


    /// Collect positions in the sort description for each key.
    const auto & left_indexes = getSortIndexes(left_data_stream, NameSet(left_key_names.begin(), left_key_names.end()));
    const auto & right_indexes = getSortIndexes(right_data_stream, NameSet(right_key_names.begin(), right_key_names.end()));

    std::vector<std::pair<String, String>> keys;
    for (size_t i = 0; i < keys_size; ++i)
        keys.emplace_back(left_key_names[i], right_key_names[i]);

    /// Sort keys pairwise according to the order in the sorting prefix.
    std::stable_sort(keys.begin(), keys.end(), [&left_indexes, &right_indexes](const auto & key1, const auto & key2)
    {
        if (getIdxFromNameMap(left_indexes, key1.first) < getIdxFromNameMap(left_indexes, key2.first))
            return true;
        return getIdxFromNameMap(right_indexes, key1.second) < getIdxFromNameMap(right_indexes, key2.second);
    });

    for (const auto & key : keys)
    {
        left_sort_descr.emplace_back(key.first);
        right_sort_descr.emplace_back(key.second);
    }
}

}
