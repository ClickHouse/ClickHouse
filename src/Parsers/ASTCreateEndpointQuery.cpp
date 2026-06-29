#include <Parsers/ASTCreateEndpointQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateEndpointQuery::clone() const
{
    return make_intrusive<ASTCreateEndpointQuery>(*this);
}

void ASTCreateEndpointQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "CREATE ENDPOINT ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(endpoint_name);
    ostr << " PROPERTIES (";
    for (size_t i = 0; i < properties.size(); ++i)
    {
        if (i)
            ostr << ", ";
        const auto & ch = properties[i];
        formatSettingName(ch.name, ostr);
        if (s.show_secrets)
            ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
        else
            ostr << " = '[HIDDEN]'";
    }
    ostr << ")";
}

}
