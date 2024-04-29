#include <Databases/removeWhereConditionPlaceholder.h>

namespace DB
{

std::string removeWhereConditionPlaceholder(const std::string & query)
{
    static constexpr auto true_condition = "(1 = 1)";
    auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);

    // We use a mutable copy of the query to perform replacements
    auto query_copy = query;

    // Loop until no placeholders are found
    while (condition_position != std::string::npos)
    {
        query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), true_condition);
        // Update the position for the next occurrence
        condition_position = query_copy.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE, condition_position + true_condition.size());
    }

    return query_copy;
}

}
