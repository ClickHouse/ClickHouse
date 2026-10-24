#include <Parsers/Access/ASTCheckGrantQuery.h>

#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

String ASTCheckGrantQuery::getID(char) const
{
    return "CheckGrantQuery";
}


ASTPtr ASTCheckGrantQuery::clone() const
{
    auto res = make_intrusive<ASTCheckGrantQuery>(*this);

    return res;
}


void ASTCheckGrantQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// `access_rights_elements` is an `AccessRightsElements` (not an AST), so the base tree hash
    /// cannot see it. Fold exactly the text the formatter emits for it (see the header comment for
    /// why the rewrite-rule matcher needs this), so the hash survives the format -> parse
    /// round-trip that the debug-build AST consistency check requires.
    WriteBufferFromOwnString buf;
    access_rights_elements.formatElementsWithoutOptions(buf);
    hash_state.update(buf.str());
}


void ASTCheckGrantQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "CHECK GRANT ";
    access_rights_elements.formatElementsWithoutOptions(ostr);
}


void ASTCheckGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}

}
