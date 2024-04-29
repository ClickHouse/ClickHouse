#include <Databases/removeWhereConditionPlaceholder.h>

namespace DB
{

std::string removeWhereConditionPlaceholder(const std::string & query)
{
    static const std::string true_condition = "(1 = 1)";
    size_t condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);

    auto query_copy = query;

    while (condition_position != std::string::npos)
    {
        query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), true_condition);
        condition_position = query_copy.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE, condition_position + true_condition.size());
    }

    return query_copy;
}

}
