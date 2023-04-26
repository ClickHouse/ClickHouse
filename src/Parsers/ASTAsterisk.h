#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** SELECT * is expanded to all visible columns of the source table.
  * Optional transformers can be attached to further manipulate these expanded columns.
  */
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
