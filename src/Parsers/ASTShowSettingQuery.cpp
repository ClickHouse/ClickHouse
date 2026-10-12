#include <Parsers/ASTShowSettingQuery.h>

#include <iomanip>
#include <IO/Operators.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTShowSettingQuery::clone() const
{
    auto res = make_intrusive<ASTShowSettingQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    res->setting_name = setting_name;
    return res;
}

void ASTShowSettingQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold in the selected setting name, which is not part of `children` (the base implementation
    /// only hashes `getID`), so `SHOW SETTING a` and `SHOW SETTING b` do not share a tree hash —
    /// see the header comment.
    hash_state.update(setting_name);
}

void ASTShowSettingQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW SETTING " << backQuoteIfNeed(setting_name);
}

}
