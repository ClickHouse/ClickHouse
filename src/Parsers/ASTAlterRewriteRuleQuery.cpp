#include <Common/quoteString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

ASTPtr ASTAlterRewriteRuleQuery::clone() const
{
    auto res = make_intrusive<ASTAlterRewriteRuleQuery>(*this);
    res->source_query = this->source_query->clone();
    if (rewrite())
    {
        res->resulting_query = this->resulting_query->clone();
    }
    return res;
}

void ASTAlterRewriteRuleQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(rule_name);
    hash_state.update(is_reject);
    hash_state.update(reject_message);
    hash_state.update(source_query != nullptr);
    if (source_query)
        source_query->updateTreeHash(hash_state, ignore_aliases);
    hash_state.update(resulting_query != nullptr);
    if (resulting_query)
        resulting_query->updateTreeHash(hash_state, ignore_aliases);
}

void ASTAlterRewriteRuleQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "ALTER RULE ";
    ostr << backQuoteIfNeed(rule_name);
    ostr << " AS ";
    ostr << "(";
    source_query->format(ostr, settings);
    ostr << ") ";
    if (rewrite())
    {
        ostr << "REWRITE TO ";
        ostr << "(";
        resulting_query->format(ostr, settings);
        ostr << ")";
    } else if (reject())
    {
        ostr << "REJECT WITH ";
        ostr << quoteString(reject_message);
    }
}

}
