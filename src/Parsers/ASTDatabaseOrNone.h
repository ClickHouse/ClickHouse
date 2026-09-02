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
    ASTPtr clone() const override { return make_intrusive<ASTDatabaseOrNone>(*this); }

    /// `none` and `database_name` are kept in plain members outside `children` and `getID` is
    /// constant, so `getTreeHash` would otherwise treat `DEFAULT DATABASE db1` and
    /// `DEFAULT DATABASE db2` (or `DEFAULT DATABASE NONE`) as equal. Fold them into the hash.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}


