#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

/** EXECUTE AS <user> [SETTINGS name = value, ...]
  *   or
  * EXECUTE AS <user> [SETTINGS name = value, ...] <subquery>
  */
class ASTExecuteAsQuery : public ASTQueryWithOutput
{
public:
    /// Owning: these are also kept in `children`, and must own them (a non-owning pointer would
    /// dangle if a child slot is replaced, e.g. by the AST fuzzer).
    ASTPtr target_user;
    /// An `ASTSetQuery` with the values for settings of the target user, or null.
    ASTPtr target_settings;
    ASTPtr subquery;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

};

}
