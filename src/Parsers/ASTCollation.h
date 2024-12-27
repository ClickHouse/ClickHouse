#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTCollation : public IAST
{
public:
    ASTPtr collation = nullptr;

    String getID(char) const override { return "Collation"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
