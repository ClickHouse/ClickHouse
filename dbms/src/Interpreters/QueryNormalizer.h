#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

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


class ASTFunction;
class ASTIdentifier;
class ASTExpressionList;
struct ASTTablesInSelectQueryElement;


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
    using TableWithColumnNames = std::pair<DatabaseAndTableWithAlias, Names>;

    struct Data
    {
        using SetOfASTs = std::set<const IAST *>;
        using MapOfASTs = std::map<ASTPtr, ASTPtr>;

        const Aliases & aliases;
        const ExtractedSettings settings;
        const std::vector<TableWithColumnNames> tables_with_columns;

        /// tmp data
        size_t level;
        MapOfASTs finished_asts;    /// already processed vertices (and by what they replaced)
        SetOfASTs current_asts;     /// vertices in the current call stack of this method
        std::string current_alias;  /// the alias referencing to the ancestor of ast (the deepest ancestor with aliases)

        Data(const Aliases & aliases_, ExtractedSettings && settings_, std::vector<TableWithColumnNames> && tables_with_columns_ = {})
            : aliases(aliases_)
            , settings(settings_)
            , tables_with_columns(tables_with_columns_)
            , level(0)
        {}

        bool processAsterisks() const { return !tables_with_columns.empty(); }
    };

    QueryNormalizer(Data & data)
        : visitor_data(data)
    {}

    void visit(ASTPtr & ast)
    {
        visit(ast, visitor_data);
    }

private:
    Data & visitor_data;

    static void visit(ASTPtr & query, Data & data);

    static void visit(ASTIdentifier &, ASTPtr &, Data &);
    static void visit(ASTFunction &, const ASTPtr &, Data &);
    static void visit(ASTExpressionList &, const ASTPtr &, Data &);
    static void visit(ASTTablesInSelectQueryElement &, const ASTPtr &, Data &);
    static void visit(ASTSelectQuery &, const ASTPtr &, Data &);

    static void visitChildren(const ASTPtr &, Data & data);
};

}
