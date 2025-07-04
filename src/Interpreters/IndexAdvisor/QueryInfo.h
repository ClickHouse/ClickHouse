#pragma once
#include <Core/Types.h>
#include <Core/Types_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/IndexAdvisor/CollectTablesMatcher.h>
namespace DB
{

class QueryInfo
{
public:
    explicit QueryInfo(const String& path, ContextMutablePtr context);

    const Strings& getWorkload() const { return queries; }
    const std::unordered_set<String>& getColumns(const String& table) const
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

    const Strings& getCreateViews() const { return create_view_queries; }
    const Strings& getDropViews() const { return drop_view_queries; }

private:
    void readQueries(const String& path);
    void parseColumnsFromQuery(const String& query);
    void processTablesAndColumns(const CollectTablesMatcher::Data& tables_data, const std::set<String>* cte_names, const ASTPtr& ast);
    Strings queries;
    std::unordered_map<String, std::unordered_set<String>> tables_to_columns;
    std::vector<String> drop_view_queries;
    std::vector<String> create_view_queries;

    ContextMutablePtr context;
    const std::unordered_set<String> EMPTY = {};
};
    
} // namespace DB
