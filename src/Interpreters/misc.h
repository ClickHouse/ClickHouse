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
    return name == "like" || name == "ilike" || name == "notLike" || name == "notILike";
}

inline bool functionIsJoinGet(const std::string & name)
{
    return name == "joinGet" || startsWith(name, "dictGet");
}

inline bool functionIsDictGet(const std::string & name)
{
    return startsWith(name, "dictGet") || (name == "dictHas") || (name == "dictIsIn");
}

}
