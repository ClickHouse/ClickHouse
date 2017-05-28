#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** Base class for AST, which can contain an alias (identifiers, literals, functions).
  */
class ASTWithAlias : public IAST
{
public:
    /// The alias, if any, or an empty string.
    String alias;

    using IAST::IAST;

    String getAliasOrColumnName() const override     { return alias.empty() ? getColumnName() : alias; }
    String tryGetAlias() const override             { return alias; }
    void setAlias(const String & to) override         { alias = to; }

    /// Calls formatImplWithoutAlias, and also outputs an alias. If necessary, encloses the entire expression in brackets.
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override final;

    virtual void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias)
{
    ast->setAlias(alias);
    return ast;
};


}
