#include <Common/ColumnsHashingImpl.h>

namespace DB
{

namespace ColumnsHashing
{

namespace columns_hashing_impl
{

// Compares two rows by ORDER BY rules from the query
// lhs and rhs - lists of GROUP BY expressions
// optimization_indexes - list of ORDER BY expressions
bool compareOrderbyFields(const std::vector<DB::Field> & lhs, const std::vector<DB::Field> & rhs, const std::vector<OptimizationDataOneExpression> & optimization_indexes)
{
    chassert(lhs.size() == rhs.size());
    for (const auto& optimization_index : optimization_indexes)
    {
        size_t index_in_groupby = optimization_index.index_of_expression_in_group_by;
        chassert(index_in_groupby < lhs.size());
        if (optimization_index.sort_direction == DB::SortDirection::ASCENDING)
        {
            if (lhs[index_in_groupby] < rhs[index_in_groupby])
                return true;
            if (lhs[index_in_groupby] > rhs[index_in_groupby])
                return false;
            continue;
        }
        if (lhs[index_in_groupby] > rhs[index_in_groupby])
            return true;
        if (lhs[index_in_groupby] < rhs[index_in_groupby])
            return false;
    }
    return false;
}

}

}

}
