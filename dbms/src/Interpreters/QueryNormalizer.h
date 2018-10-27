#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Interpreters/evaluateQualified.h>

namespace DB
{

inline bool functionIsInOperator(const String & name)
{
    return name == "in" || name == "notIn";
}

inline bool functionIsInOrGlobalInOperator(const String & name)
{
    return functionIsInOperator(name) || name == "globalIn" || name == "globalNotIn";
}


using TableNameAndColumnNames = std::pair<DatabaseAndTableWithAlias, Names>;
using TableNamesAndColumnNames = std::vector<TableNameAndColumnNames>;


class QueryNormalizer
{
    /// Extracts settings, mostly to show which are used and which are not.
    struct ExtractedSettings
    {
        const UInt64 max_ast_depth;
        const UInt64 max_expanded_ast_elements;
        const String count_distinct_implementation;

        template <typename T>
        ExtractedSettings(const T & settings)
        :   max_ast_depth(settings.max_ast_depth),
            max_expanded_ast_elements(settings.max_expanded_ast_elements),
            count_distinct_implementation(settings.count_distinct_implementation)
        {}
    };

public:
    using Aliases = std::unordered_map<String, ASTPtr>;

    QueryNormalizer(ASTPtr & query, const Aliases & aliases, ExtractedSettings && settings, const Names & all_columns_name,
                    const TableNamesAndColumnNames & table_names_and_column_names);

    void perform();

private:
    using SetOfASTs = std::set<const IAST *>;
    using MapOfASTs = std::map<ASTPtr, ASTPtr>;

    ASTPtr & query;
    const Aliases & aliases;
    const ExtractedSettings settings;
    const Names & all_column_names;
    const TableNamesAndColumnNames & table_names_and_column_names;

    void performImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, size_t level);
};

}
