#pragma once

#include <string>

namespace DB
{

enum SetType
{
    SET,
    BLOOM_FILTER,
    CUCKOO_FILTER,
};

SetType getSetTypeFromFunctionInName(const std::string & function_name);

}
