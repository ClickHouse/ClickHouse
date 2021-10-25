#pragma once

#include "IAST.h"

namespace DB
{

class ASTDropFunctionQuery : public IAST
{
public:
    String function_name;

    bool if_exists = false;

    String getID(char) const override { return "DropFunctionQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
