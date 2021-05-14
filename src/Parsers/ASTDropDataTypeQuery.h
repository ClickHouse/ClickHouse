#pragma once

#include "IAST.h"

namespace DB
{

class ASTDropDataTypeQuery : public IAST
{
public:
    String type_name;

    String getID(char) const override { return "DropDataTypeQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
