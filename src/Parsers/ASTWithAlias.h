#pragma once

#include <Core/IdentifierName.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTQueryParameter;

/** Base class for AST, which can contain an alias (identifiers, literals, functions).
  */
class ASTWithAlias : public IAST
{
protected:
    struct ASTWithAliasFlags
    {
        using ParentFlags = void;
        static constexpr UInt32 RESERVED_BITS = 1;

        UInt32 prefer_alias_to_column_name : 1;
        UInt32 unused : 31;
    };

public:
    ASTWithAlias();
    ASTWithAlias(const ASTWithAlias &);
    ~ASTWithAlias() override;
    ASTWithAlias & operator=(const ASTWithAlias &);

    /// The alias, if any, or an empty string.
    String alias;

    /// Quoting of the alias as written in the query. Double quotes pin the alias to
    /// exact-case matching under `standard` name matching.
    IdentifierPartQuote alias_quote = IdentifierPartQuote::Unquoted;

    /// If is true, getColumnName returns alias. Uses for aliases in former WITH section of SELECT query.
    /// Example: 'WITH pow(2, 2) as a SELECT pow(a, 2)' returns 'pow(a, 2)' instead of 'pow(pow(2, 2), 2)'
    bool preferAliasToColumnName() const { return flags<ASTWithAliasFlags>().prefer_alias_to_column_name; }
    void setPreferAliasToColumnName(bool value) { flags<ASTWithAliasFlags>().prefer_alias_to_column_name = value; }
    // An alias can be defined as a query parameter,
    // in which case we can only resolve it during query execution.
    boost::intrusive_ptr<ASTQueryParameter> parametrised_alias;

    using IAST::IAST;

    void appendColumnName(WriteBuffer & ostr) const final;
    void appendColumnNameWithoutAlias(WriteBuffer & ostr) const final;
    String getAliasOrColumnName() const override { return alias.empty() ? getColumnName() : alias; }
    String tryGetAlias() const override { return alias; }

    /// The quote flag is always updated together with the alias string.
    void setAlias(const String & to) override
    {
        alias = to;
        alias_quote = IdentifierPartQuote::Unquoted;
    }

    void setAlias(const String & to, IdentifierPartQuote quote)
    {
        alias = to;
        alias_quote = quote;
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    virtual void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

protected:
    /// Calls formatImplWithoutAlias, and also outputs an alias. If necessary, encloses the entire expression in brackets.
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const final;

    virtual void appendColumnNameImpl(WriteBuffer & ostr) const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias)
{
    ast->setAlias(alias);
    return ast;
}

/// Quoting of the node's alias, or `Unquoted` for nodes that cannot carry an alias.
inline IdentifierPartQuote tryGetAliasQuote(const IAST * ast)
{
    if (const auto * ast_with_alias = dynamic_cast<const ASTWithAlias *>(ast))
        return ast_with_alias->alias_quote;
    return IdentifierPartQuote::Unquoted;
}

inline IdentifierPartQuote tryGetAliasQuote(const ASTPtr & ast)
{
    return tryGetAliasQuote(ast.get());
}


}
