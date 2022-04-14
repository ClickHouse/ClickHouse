#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTCollation : public IAST
{
public:
    ASTPtr collation = nullptr;
    std::optional<bool> null_modifier;


    String getID(char) const override { return "Collation"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
