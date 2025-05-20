#pragma once

#include <Core/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/IndexAdvisor/QueryInfo.h>

namespace DB
{

using IndexType = std::pair<String, String>;
using IndexTypes = std::vector<IndexType>;

class IndexAdvisor
{
public:
    IndexAdvisor(ContextMutablePtr context_, QueryInfo & workload_)
        : workload(workload_)
        , context(context_)
    {
    }

    IndexTypes getBestMinMaxIndexForTable(const String & table);
    std::unordered_map<String, IndexTypes> getBestMinMaxIndexForTables();

    std::pair<Strings, UInt64> getBestPKColumnsForTable(const String & table);
    std::unordered_map<String, std::pair<Strings, UInt64>> getBestPKColumns();
   
private:
    QueryInfo & workload;
    ContextMutablePtr context;

    static constexpr std::array<const char*, 5> index_types = {"minmax", "set(256)", "bloom_filter(0.01)", "tokenbf_v1(2048, 3, 1)", "ngrambf_v1(3, 2048, 3, 1)"};
};

}
