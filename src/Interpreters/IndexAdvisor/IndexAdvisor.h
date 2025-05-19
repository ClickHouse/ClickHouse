#pragma once

#include <Core/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/IndexAdvisor/QueryInfo.h>

namespace DB
{

class IndexAdvisor
{
public:
    IndexAdvisor(ContextMutablePtr context_, QueryInfo & workload_)
        : workload(workload_)
        , context(context_)
    {
    }

    std::pair<Strings, UInt64> getBestPKColumnsForTable(const String & table);
    std::unordered_map<String, std::pair<Strings, UInt64>> getBestPKColumns();

private:
    QueryInfo & workload;
    ContextMutablePtr context;
};

}
