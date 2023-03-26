#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** name CHECK logical_expr
 */
class ASTConstraintDeclaration : public IAST
{
public:
    enum class Type : UInt8
    {
        CHECK,
        ASSUME,
    };

    String name;
    Type type;
    IAST * expr;

    String getID(char) const override { return "Constraint"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};
}
