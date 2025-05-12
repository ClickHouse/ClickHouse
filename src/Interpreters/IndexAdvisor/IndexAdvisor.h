#pragma once
#include "TableManager.h"

namespace DB
{

class IndexAdvisor
{
public:
    explicit IndexAdvisor(TableManager & table_manager_, ContextMutablePtr context_)
        : table_manager(table_manager_), context(context_) {}

    std::unordered_map<String, Strings> getBestPKColumns();
private:
    Strings getBestPKColumnsForTable(const String & table);

    TableManager & table_manager;
    ContextMutablePtr context;
};

}
