#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

ASTPtr ASTCreateNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTCreateNamedCollectionQuery>(*this);
}

void ASTCreateNamedCollectionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE NAMED COLLECTION ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(collection_name);

    formatOnCluster(ostr, settings);

    ostr << " AS ";
    bool first = true;
    for (const auto & change : changes)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, ostr);

        if (settings.show_secrets)
            ostr << " = " << applyVisitor(FieldVisitorToString(), change.value);
        else
            ostr << " = '[HIDDEN]'";
        auto override_value = overridability.find(change.name);
        if (override_value != overridability.end())
            ostr << " " << (override_value->second ? "" : "NOT ") << "OVERRIDABLE";
    }
}

}
