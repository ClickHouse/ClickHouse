#pragma once

#include <Parsers/IAST.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** EXECUTE AS <user>
  */
class ASTExecuteAsQuery : public IAST
{
public:

    std::shared_ptr<ASTRolesOrUsersSet> targetuser;

    String getID(char) const override;
    ASTPtr clone() const override;
protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

};
}
