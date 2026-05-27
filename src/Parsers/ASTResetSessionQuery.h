#pragma once

#include <Parsers/IAST.h>
#include <IO/Operators.h>


namespace DB
{

/** RESET SESSION query.
  *
  * Restores the current TCP session to the state it was in immediately after
  * authentication: settings, profiles, roles and the current database are
  * re-derived from access control, and temporary tables, query parameters
  * and scalars created during the session are dropped. The authenticated
  * user identity itself is preserved.
  */
class ASTResetSessionQuery : public IAST
{
public:
    String getID(char) const override { return "ResetSessionQuery"; }

    ASTPtr clone() const override
    {
        return make_intrusive<ASTResetSessionQuery>(*this);
    }

    QueryKind getQueryKind() const override { return QueryKind::ResetSession; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override
    {
        ostr << "RESET SESSION";
    }
};

}
