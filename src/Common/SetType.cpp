#include <Common/SetType.h>

#include <Analyzer/Utils.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

SetType getSetTypeFromFunctionInName(const std::string & function_name)
{
    if (!isNameOfInFunction(function_name))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} is not a IN function name", function_name);
    }

    if (function_name == "inBloomFilter")
    {
        return SetType::BLOOM_FILTER;
    }
    else if (function_name == "inCuckooFilter")
    {
        return SetType::CUCKOO_FILTER;
    }
    else if (function_name == "inVacuumFilter")
    {
        return SetType::VACUUM_FILTER;
    }
    else
    {
        return SetType::SET;
    }
}

}
