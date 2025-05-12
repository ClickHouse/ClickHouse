#pragma once
#include <Core/Types.h>
#include <Core/Types_fwd.h>
#include "QueryInfo.h"

namespace DB
{

class TableManager
{
public:
    explicit TableManager(const QueryInfo & workload_, ContextMutablePtr context_)
        : workload(workload_), context(context_) {}

    Strings getTables() const
    {
        return workload.getTables();
    }

    Strings getColumns(const String & table) const
    {
        return workload.getColumns(table);
    }

    UInt64 estimate(std::unordered_map<String, Strings> & pk_columns);

private:
    void buildTable(const String& table, const Strings & pk_columns);
    UInt64 estimateQueries();

    QueryInfo workload;
    ContextMutablePtr context;
    std::unordered_map<String, String> replacement_tables;
};

}
