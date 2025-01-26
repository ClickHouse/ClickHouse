#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{
class ASTRolesOrUsersSet;

/** EXECUTE AS <user>
  */
class ASTExecuteAsQuery : public ASTQueryWithOutput
{
public:

    std::shared_ptr<ASTRolesOrUsersSet> targetuser;

    ASTSelectWithUnionQuery * select = nullptr;

    String getID(char) const override;
    ASTPtr clone() const override;
protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

};
}
