#pragma once

#include "Types.h"

namespace DB
{

ValueMatcherPtr MakeAlwaysTrueMatcher();
ValueMatcherPtr MakeExactMatcher(std::string_view pattern);
ValueMatcherPtr MakeStartsWithMatcher(std::string_view pattern);
ValueMatcherPtr MakeEndsWithMatcher(std::string_view pattern);
ValueMatcherPtr MakeContainsWordMatcher(std::string_view pattern);
ValueMatcherPtr MakeSubstringMatcher(std::string_view pattern);

}
