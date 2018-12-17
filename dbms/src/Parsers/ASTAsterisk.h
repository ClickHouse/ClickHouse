#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTAsterisk : public IAST
{
public:
    String getID(char) const override { return "Asterisk"; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
