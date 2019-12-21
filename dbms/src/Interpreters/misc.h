#pragma once

#include <Common/StringUtils/StringUtils.h>

namespace DB
{

inline bool functionIsInOperator(const std::string & name)
{
    return name == "in" || name == "notIn";
}

inline bool functionIsInOrGlobalInOperator(const std::string & name)
{
    return functionIsInOperator(name) || name == "globalIn" || name == "globalNotIn";
}

inline bool functionIsJoinGetOrDictGet(const std::string & name)
{
    return name == "joinGet" || startsWith(name, "dictGet");
}

}
