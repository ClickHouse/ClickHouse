#pragma once

#include <Common/StringUtils/StringUtils.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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
    return startsWith(name, "joinGet");
}

inline bool functionIsDictGet(const std::string & name)
{
    return startsWith(name, "dictGet") || (name == "dictHas") || (name == "dictIsIn");
}

inline bool checkFunctionIsInOrGlobalInOperator(const ASTFunction & func)
{
    if (functionIsInOrGlobalInOperator(func.name))
    {
        size_t num_arguments = func.arguments->children.size();
        if (num_arguments != 2)
            throw Exception("Wrong number of arguments passed to function in. Expected: 2, passed: " + std::to_string(num_arguments),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return true;
    }

    return false;
}

}
