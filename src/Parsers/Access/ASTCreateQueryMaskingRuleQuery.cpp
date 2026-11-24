#include <Parsers/Access/ASTCreateQueryMaskingRuleQuery.h>
#include <Common/HiddenSecret.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

String ASTCreateQueryMaskingRuleQuery::getID(char) const
{
    return "CreateQueryMaskingRuleQuery";
}


ASTPtr ASTCreateQueryMaskingRuleQuery::clone() const
{
    return make_intrusive<ASTCreateQueryMaskingRuleQuery>(*this);
}


void ASTCreateQueryMaskingRuleQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
        ostr << "ATTACH QUERY MASKING RULE";
    else
        ostr << (alter ? "ALTER QUERY MASKING RULE" : "CREATE QUERY MASKING RULE");

    if (if_exists)
        ostr << " IF EXISTS";
    else if (if_not_exists)
        ostr << " IF NOT EXISTS";
    else if (or_replace)
        ostr << " OR REPLACE";

    ostr << " " << backQuoteIfNeed(name);

    if (!storage_name.empty())
        ostr << " IN " << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, settings);

    if (!new_name.empty())
        ostr << " RENAME TO " << backQuoteIfNeed(new_name);

    if (regexp)
    {
        ostr << " REGEXP ";
        if (settings.show_secrets)
            ostr << quoteString(*regexp);
        else
            ostr << HIDDEN_SECRET_LITERAL;
    }

    if (replace)
        ostr << " REPLACE " << quoteString(*replace);
}

}
