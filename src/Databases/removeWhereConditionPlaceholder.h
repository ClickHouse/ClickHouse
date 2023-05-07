#pragma once
#include <string>

namespace DB
{

static constexpr std::string_view CONDITION_PLACEHOLDER_TO_REPLACE_VALUE = "{condition}";

std::string removeWhereConditionPlaceholder(const std::string & query);

}
