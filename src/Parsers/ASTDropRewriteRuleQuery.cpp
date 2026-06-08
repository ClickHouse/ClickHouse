#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTDropRewriteRuleQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

ASTPtr ASTDropRewriteRuleQuery::clone() const
{
    return make_intrusive<ASTDropRewriteRuleQuery>(*this);
}

void ASTDropRewriteRuleQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings &, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP RULE ";
    ostr << backQuoteIfNeed(rule_name);
}

}
