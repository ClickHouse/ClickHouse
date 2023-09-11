#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTSetQuery.h>
#include <Core/Field.h>

namespace DB
{
/**
 * SET CONFIG [GLOBAL] name {NONE | = value}
 */
class ASTSetConfigQuery : public IAST
{
public:
    String name;
    Field value;
    bool is_global{false};
    bool is_none{false};

    String getID(char) const override { return "Set Config"; }

    ASTPtr clone() const override { return std::make_shared<ASTSetConfigQuery>(*this); }

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
