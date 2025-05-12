#pragma once
#include <Core/Types.h>
#include <Core/Types_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include "CollectTablesMatcher.h"

namespace DB
{

class QueryInfo
{
public:
    explicit QueryInfo(const String& path, ContextMutablePtr context);

    const Strings& getWorkload() const { return queries; }
    const Strings& getColumns(const String& table) const
    {
        auto it = tables_to_columns.find(table);
        if (it != tables_to_columns.end())
            return it->second;
        return EMPTY;
    }
    Strings getTables() const
    {
        Strings tables;
        for (const auto& pair : tables_to_columns)
            tables.push_back(pair.first);
        return tables;
    }

    // Get all views that were created during query processing
    const Strings& getViews() const { return views; }

private:
    void readQueries(const String& path);
    void parseColumnsFromQuery(const String& query);
    void processTablesAndColumns(const CollectTablesMatcher::Data & tables_data, const std::set<String> * cte_names = nullptr, const ASTPtr & ast = nullptr);

    Strings queries;
    std::unordered_map<String, Strings> tables_to_columns;
    Strings views;  // Store names of views that were created
    std::vector<String> drop_view_queries;

    ContextMutablePtr context;
    const Strings EMPTY = {};
};
    
} // namespace DB
