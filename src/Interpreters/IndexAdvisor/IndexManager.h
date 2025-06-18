#pragma once
#include <Interpreters/Context.h>
#include "QueryInfo.h"

namespace DB
{


class IndexManager
{
public:
    IndexManager(ContextMutablePtr context_, QueryInfo & workload_, const String & table_)
        : workload(workload_)
        , context(context_)
        , table(table_)
    {
    }

    bool addIndex(const String & index_name, const String & index_columns, const String & index_type);
    void dropIndex(const String & index_name);
    Int64 estimate();
    std::unordered_set<String> getColumns() { return workload.getColumns(table); }
private:
    QueryInfo workload;
    ContextMutablePtr context;
    String table;
};

}
