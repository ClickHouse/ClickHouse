#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTQueryParameter;

/** Base class for AST, which can contain an alias (identifiers, literals, functions).
  */
class ASTWithAlias : public IAST
{
public:
    /// The alias, if any, or an empty string.
    String alias;
    /// If is true, getColumnName returns alias. Uses for aliases in former WITH section of SELECT query.
    /// Example: 'WITH pow(2, 2) as a SELECT pow(a, 2)' returns 'pow(a, 2)' instead of 'pow(pow(2, 2), 2)'
    bool prefer_alias_to_column_name = false;
    // An alias can be defined as a query parameter,
    // in which case we can only resolve it during query execution.
    std::optional<std::shared_ptr<ASTQueryParameter>> parametrised_alias;

    using IAST::IAST;

    void appendColumnName(WriteBuffer & ostr) const final;
    void appendColumnNameWithoutAlias(WriteBuffer & ostr) const final;
    String getAliasOrColumnName() const override { return alias.empty() ? getColumnName() : alias; }
    String tryGetAlias() const override { return alias; }
    void setAlias(const String & to) override { alias = to; }

    /// Calls formatImplWithoutAlias, and also outputs an alias. If necessary, encloses the entire expression in brackets.
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const final;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    virtual void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

protected:
    virtual void appendColumnNameImpl(WriteBuffer & ostr) const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias)
{
    ast->setAlias(alias);
    return ast;
}


}
