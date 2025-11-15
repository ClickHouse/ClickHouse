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
    std::cout << "compareOrderbyFields called, lhs.size()=" << lhs.size() << "; rhs.size()=" << rhs.size() << "; optimization_indexes.size()=" << optimization_indexes.size() << std::endl;
    chassert(lhs.size() == rhs.size());
    for (size_t i = 0; i < optimization_indexes.size(); ++i)
    {
        std::cout << "optimization_indexes[i].index_of_expression_in_group_by: " << optimization_indexes[i].index_of_expression_in_group_by << std::endl;
        size_t index_in_groupby = optimization_indexes[i].index_of_expression_in_group_by;
        chassert(index_in_groupby < lhs.size());
        if (optimization_indexes[i].sort_direction == DB::SortDirection::ASCENDING)
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
