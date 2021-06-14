#pragma once

#include "IAST.h"

namespace DB
{

class ASTCreateDataTypeQuery : public IAST
{
public:
    String type_name;
    ASTPtr nested;

    String getID(char) const override { return "CreateDataTypeQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
