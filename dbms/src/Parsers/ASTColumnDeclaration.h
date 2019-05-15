#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** Name, type, default-specifier, default-expression, comment-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
    String name;
    ASTPtr type;
    String default_specifier;
    ASTPtr default_expression;
    ASTPtr codec;
    ASTPtr comment;
    ASTPtr ttl;

    String getID(char delim) const override { return "ColumnDeclaration" + (delim + name); }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
