#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
class ASTUserNameWithHost;

/** EXECUTE AS <user>
  *   or
  * EXECUTE AS <user> <subquery>
  */
class ASTExecuteAsQuery : public ASTQueryWithOutput
{
public:
    ASTUserNameWithHost * target_user;
    IAST * subquery = nullptr;

    String getID(char) const override;
    ASTPtr clone() const override;
protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

};

}
