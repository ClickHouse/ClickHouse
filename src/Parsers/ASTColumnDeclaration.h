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
    std::optional<bool> null_modifier;
    String default_specifier;
    ASTPtr default_expression;
    bool ephemeral_default = false;
    ASTPtr comment;
    ASTPtr codec;
    ASTPtr statistics_desc;
    ASTPtr ttl;
    ASTPtr collation;
    ASTPtr settings;
    bool primary_key_specifier = false;

    String getID(char delim) const override { return "ColumnDeclaration" + (delim + name); }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const override;
};

}
