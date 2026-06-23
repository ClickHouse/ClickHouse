#include <Parsers/ASTAlterEndpointQuery.h>

#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/formatSettingName.h>


namespace DB
{

ASTPtr ASTAlterEndpointQuery::clone() const
{
    return make_intrusive<ASTAlterEndpointQuery>(*this);
}

void ASTAlterEndpointQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "ALTER ENDPOINT " << backQuoteIfNeed(endpoint_name) << " MODIFY PROPERTIES (";
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
