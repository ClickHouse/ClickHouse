#include <Databases/removeWhereConditionPlaceholder.h>

namespace DB
{

std::string removeWhereConditionPlaceholder(const std::string &query)
{
    static constexpr auto true_condition = "(1 = 1)";
    std::string modified_query = query;
    size_t condition_position = modified_query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);

    while (condition_position != std::string::npos)
    {
        modified_query.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), true_condition);
        condition_position = modified_query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE, condition_position + true_condition.size());
    }

    return modified_query;
}

}
