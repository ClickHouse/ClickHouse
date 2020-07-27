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
    return name == "in" || name == "notIn";
}

inline bool functionIsInOrGlobalInOperator(const std::string & name)
{
    return functionIsInOperator(name) || name == "globalIn" || name == "globalNotIn";
}

inline bool functionIsLikeOperator(const std::string & name)
{
    return name == "like" || name == "notLike";
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
