#include <Interpreters/FullSortingMergeJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void assertIsPermutation(const std::vector<size_t> & permutation)
{
    std::set<size_t> sorted(permutation.begin(), permutation.end());
    if (sorted.size() != permutation.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Permutation is not unique");

    for (size_t i = 0; i < permutation.size(); ++i)
    {
        if (sorted.count(i) != 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Permutation is not complete");
        }
    }
}

void FullSortingMergeJoin::permuteKeys(const std::vector<size_t> & permutation)
{
    auto & join_on = table_join->getOnlyClause();

    assert(permutation.size() == join_on.key_names_left.size() &&
           permutation.size() == join_on.key_names_right.size());

    assertIsPermutation(permutation);

    Names new_key_names_left;
    Names new_key_names_right;
    for (size_t idx : permutation)
    {
        new_key_names_left.push_back(join_on.key_names_left[idx]);
        new_key_names_right.push_back(join_on.key_names_right[idx]);
    }
    join_on.key_names_left = std::move(new_key_names_left);
    join_on.key_names_right = std::move(new_key_names_right);
}

const Names & FullSortingMergeJoin::getKeyNames(JoinTableSide side) const
{
    auto & join_on = table_join->getOnlyClause();
    return side == JoinTableSide::Left ? join_on.key_names_left : join_on.key_names_right;
}

void FullSortingMergeJoin::setPrefixSortDesctiption(const SortDescription & prefix_sort_description_, JoinTableSide side)
{
    if (side == JoinTableSide::Left)
        left_prefix_sort_description = prefix_sort_description_;
    else
        right_prefix_sort_description = prefix_sort_description_;
}

SortDescription FullSortingMergeJoin::getPrefixSortDesctiption(JoinTableSide side) const
{
    return side == JoinTableSide::Left ? left_prefix_sort_description : right_prefix_sort_description;
}

}
