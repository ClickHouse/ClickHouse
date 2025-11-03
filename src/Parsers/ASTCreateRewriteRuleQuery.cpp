#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{
// todo deep copy
ASTPtr ASTCreateRewriteRuleQuery::clone() const
{
    auto res = std::make_shared<ASTCreateRewriteRuleQuery>(*this);
    res->source_query = this->source_query->clone();
    if (rewrite())
    {
        res->resulting_query = this->resulting_query->clone();
    }
    return res;
}

void ASTCreateRewriteRuleQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE RULE ";
    ostr << rule_name;
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
        ostr << reject_message;
    }
}

}
