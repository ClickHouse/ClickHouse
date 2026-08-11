#include <Storages/MergeTree/WhatIfSupportedIndexTypes.h>

namespace DB
{

namespace
{

/// `IndexDescription::getIndexFromAST` lowercases the type, so these are compared as they are
const char * const supported_index_types[]
{
    "bloom_filter",
    "minmax",
    "ngrambf_v1",
    "set",
    "sparse_grams",
    "tokenbf_v1",
};

}

bool isIndexTypeSupportedByWhatIf(const String & index_type)
{
    for (const auto * supported : supported_index_types)
    {
        if (index_type == supported)
            return true;
    }
    return false;
}

String getIndexTypesSupportedByWhatIf()
{
    String result;
    for (const auto * supported : supported_index_types)
    {
        if (!result.empty())
            result += ", ";
        result += supported;
    }
    return result;
}

}
