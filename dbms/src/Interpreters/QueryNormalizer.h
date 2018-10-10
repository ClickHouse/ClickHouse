#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Settings.h>
#include <Interpreters/evaluateQualified.h>

namespace DB
{

using TableNameAndColumnNames = std::pair<DatabaseAndTableWithAlias, Names>;
using TableNamesAndColumnNames = std::vector<TableNameAndColumnNames>;

class QueryNormalizer
{
public:
    using Aliases = std::unordered_map<String, ASTPtr>;

    QueryNormalizer(ASTPtr & query, const Aliases & aliases, const Settings & settings, const Names & all_columns_name,
                    const TableNamesAndColumnNames & table_names_and_column_names);

    void perform();

private:
    using SetOfASTs = std::set<const IAST *>;
    using MapOfASTs = std::map<ASTPtr, ASTPtr>;

    ASTPtr & query;
    const Aliases & aliases;
    const Settings & settings;
    const Names & all_column_names;
    const TableNamesAndColumnNames & table_names_and_column_names;

    void performImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, size_t level);
};

}
