#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <optional>
#include <vector>


namespace DB
{

/// CREATE [IF NOT EXISTS] HANDLER name [PROTOCOL p] URL [PREFIX|REGEXP] '/x' [METHODS (GET, POST)] [TYPE query] AS SELECT ...
/// ALTER HANDLER name [PROTOCOL p] [URL ...] [METHODS ...] [TYPE ...] [AS ...]
///
/// The same AST is used for CREATE and ALTER. For ALTER (is_alter == true) the clauses are optional and
/// only the specified ones are updated; the unspecified clauses keep their previous values.
class ASTCreateHandlerQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum class URLMatchType
    {
        Exact,
        Prefix,
        Regexp,
    };

    String handler_name;

    /// Whether this is an ALTER HANDLER (partial update) or CREATE HANDLER.
    bool is_alter = false;
    bool if_not_exists = false;

    /// Optional PROTOCOL clause. std::nullopt - not specified.
    std::optional<String> protocol;

    /// URL clause. Mandatory for CREATE, optional for ALTER.
    bool has_url = false;
    URLMatchType url_match_type = URLMatchType::Exact;
    String url;

    /// Optional METHODS clause. std::nullopt - not specified (defaults to GET on CREATE).
    std::optional<std::vector<String>> methods;

    /// Optional TYPE clause. Only "query" is supported. std::nullopt - not specified.
    std::optional<String> handler_type;

    /// The query to be executed by the handler (the part after AS). Stored as a child.
    ASTPtr query;

    String getID(char) const override { return "CreateHandlerQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateHandlerQuery>(clone());
    }

    QueryKind getQueryKind() const override { return is_alter ? QueryKind::Alter : QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
