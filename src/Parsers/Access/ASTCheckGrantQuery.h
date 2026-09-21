#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AccessRightsElement.h>


namespace DB
{

/** Parses queries like
  * CHECK GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}
  */
class ASTCheckGrantQuery : public IAST
{
public:
    AccessRightsElements access_rights_elements;

    String getID(char) const override;
    ASTPtr clone() const override;

    /// `getID` returns a constant string, and `access_rights_elements` is a plain member (not an
    /// AST), not part of `children` (this AST has none). Without folding it into the hash,
    /// `CHECK GRANT SELECT ON a` and `CHECK GRANT SELECT ON b` would share one tree hash. The
    /// rewrite-rule matcher treats an equal tree hash as semantic equality, so a rule template
    /// for one `CHECK GRANT` would over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void replaceEmptyDatabase(const String & current_database);
    QueryKind getQueryKind() const override { return QueryKind::Check; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
