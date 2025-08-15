#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTDatabaseOrNone : public IAST
{
public:
    bool none = false;
    String database_name;

    bool isNone() const { return none; }
    String getID(char) const override { return "DatabaseOrNone"; }
    ASTPtr clone() const override { return std::make_shared<ASTDatabaseOrNone>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}


