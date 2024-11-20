#pragma once

#include <map>

#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Aliases.h>
#include <Parsers/IAST.h>

namespace DB
{

class ASTSelectQuery;
class ASTIdentifier;
struct ASTTablesInSelectQueryElement;
class Context;
class ASTQueryParameter;

namespace Setting
{
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_expanded_ast_elements;
    extern const SettingsBool prefer_column_name_to_alias;
}


class QueryNormalizer
{
    /// Extracts settings, mostly to show which are used and which are not.
    struct ExtractedSettings
    {
        const UInt64 max_ast_depth;
        const UInt64 max_expanded_ast_elements;
        bool prefer_column_name_to_alias;

        template <typename T>
        ExtractedSettings(const T & settings) /// NOLINT
            : max_ast_depth(settings[Setting::max_ast_depth])
            , max_expanded_ast_elements(settings[Setting::max_expanded_ast_elements])
            , prefer_column_name_to_alias(settings[Setting::prefer_column_name_to_alias])
        {
        }
    };

public:
    struct Data
    {
        using SetOfASTs = std::set<const IAST *>;
        using MapOfASTs = std::map<ASTPtr, ASTPtr>;

        Aliases & aliases;
        const NameSet & source_columns_set;
        ExtractedSettings settings;
        NameSet query_parameters;

        /// tmp data
        size_t level;
        MapOfASTs finished_asts;    /// already processed vertices (and by what they replaced)
        SetOfASTs current_asts;     /// vertices in the current call stack of this method
        std::string current_alias;  /// the alias referencing to the ancestor of ast (the deepest ancestor with aliases)
        const bool ignore_alias; /// normalize query without any aliases

        /// It's Ok to have "c + 1 AS c" in queries, but not in table definition
        const bool allow_self_aliases; /// for constructs like "SELECT column + 1 AS column"
        bool is_create_parameterized_view;

        Data(Aliases & aliases_, const NameSet & source_columns_set_, bool ignore_alias_, ExtractedSettings && settings_, bool allow_self_aliases_, bool is_create_parameterized_view_ = false)
            : aliases(aliases_)
            , source_columns_set(source_columns_set_)
            , settings(settings_)
            , level(0)
            , ignore_alias(ignore_alias_)
            , allow_self_aliases(allow_self_aliases_)
            , is_create_parameterized_view(is_create_parameterized_view_)
        {}
    };

    explicit QueryNormalizer(Data & data)
        : visitor_data(data)
    {}

    void visit(ASTPtr & ast)
    {
        visit(ast, visitor_data);
    }

private:
    Data & visitor_data;

    static void visit(ASTPtr & ast, Data & data);

    static void visit(ASTIdentifier &, ASTPtr &, Data &);
    static void visit(ASTTablesInSelectQueryElement &, const ASTPtr &, Data &);
    static void visit(ASTSelectQuery &, const ASTPtr &, Data &);

    static void visitChildren(IAST * node, Data & data);
};

}
