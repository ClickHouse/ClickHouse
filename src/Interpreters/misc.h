#pragma once

#include <Common/StringUtils/StringUtils.h>

namespace DB
{

inline bool functionIsInOperator(const std::string & name)
{
    return name == "in" || name == "notIn" || name == "nullIn" || name == "notNullIn";
}

inline bool functionIsInOrGlobalInOperator(const std::string & name)
{
    return functionIsInOperator(name) || name == "globalIn" || name == "globalNotIn" || name == "globalNullIn" || name == "globalNotNullIn";
}

inline bool functionIsLikeOperator(const std::string & name)
{
    return name == "like" || name == "notLike";
}

inline bool functionIsJoinGetOrDictGet(const std::string & name)
{
    return name == "joinGet" || startsWith(name, "dictGet");
}

}
