#pragma once
#include <string>

namespace DB
{

static constexpr std::string_view CONDITION_PLACEHOLDER_TO_REPLACE_VALUE = "{condition}";

/** In case UPDATE_FIELD is specified in {condition} for dictionary that must load all data.
  * Replace {condition} with true_condition for initial dictionary load.
  * For next dictionary loads {condition} will be updated with UPDATE_FIELD.
  */
std::string removeWhereConditionPlaceholder(const std::string & query);

}
