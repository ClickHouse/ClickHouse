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

    /// If true, appendColumnName will return alias if `prefer_alias = true` and alias is not empty.
    /// Uses for aliases in former WITH section of SELECT query.
    /// Example: 'WITH pow(2, 2) as a SELECT pow(a, 2)' returns 'pow(a, 2)' instead of 'pow(pow(2, 2), 2)'
    bool prefer_alias_to_column_name = false;

    /// If true, appendColumnName will return alias if not empty. This is used for virtual columns such as
    /// _database and _table in StorageMerge.
    bool force_alias = false;

    using IAST::IAST;

    void appendColumnName(WriteBuffer & ostr, bool prefer_alias) const final;
    String getAliasOrColumnName() const override { return alias.empty() ? getColumnName() : alias; }
    String getAliasOrColumnNamePreferAlias() const override { return alias.empty() ? getColumnName(true) : alias; }
    String tryGetAlias() const override { return alias; }
    void setAlias(const String & to) override { alias = to; }

    /// Calls formatImplWithoutAlias, and also outputs an alias. If necessary, encloses the entire expression in brackets.
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override final;

    virtual void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

protected:
    virtual void appendColumnNameImpl(WriteBuffer & ostr, bool prefer_alias) const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias)
{
    ast->setAlias(alias);
    return ast;
}


}
