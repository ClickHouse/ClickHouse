#pragma once
#include <Core/Types.h>
#include <Core/Types_fwd.h>
#include "QueryInfo.h"

namespace DB
{

class TableManager
{
public:
    explicit TableManager(ContextMutablePtr context_, const QueryInfo & workload_, const String & table_)
        : workload(workload_)
        , context(context_)
        , table(table_)
        , estimation_table(table + "_estimation")
    {
    }

    Strings getTables() const { return workload.getTables(); }

    std::unordered_set<String> getColumns() const { return workload.getColumns(table); }

    UInt64 estimate(const String & pk_columns);

private:
    void buildTable(const String & pk_columns);
    UInt64 estimateQueries();
    void dropTable(const String & target_table);
    
    QueryInfo workload;
    ContextMutablePtr context;
    String table;
    String estimation_table;
};

}
