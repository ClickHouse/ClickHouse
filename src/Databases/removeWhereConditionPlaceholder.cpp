#include <Databases/removeWhereConditionPlaceholder.h>

namespace DB
{

std::string removeWhereConditionPlaceholder(const std::string  & query)
{
    /** In case UPDATE_FIELD is specified in {condition} for dictionary that must load all data.
      * Replace {condition} with true_condition for initial dictionary load.
      * For next dictionary loads {condition} will be updated with UPDATE_FIELD.
      */
    static constexpr auto true_condition = "(1 = 1)";
    auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
    if (condition_position != std::string::npos)
    {
        auto query_copy = query;
        query_copy.replace(condition_position, CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size(), true_condition);
        return query_copy;
    }

    return query;
}

}
