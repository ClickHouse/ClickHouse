#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

ASTPtr ASTCreateNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTCreateNamedCollectionQuery>(*this);
}

void ASTCreateNamedCollectionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE NAMED COLLECTION ";
    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(collection_name) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    bool first = true;
    for (const auto & change : changes)
    {
        if (!first)
            settings.ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, settings.ostr);

        if (settings.show_secrets)
            settings.ostr << " = " << applyVisitor(FieldVisitorToString(), change.value);
        else
            settings.ostr << " = '[HIDDEN]'";
        auto override_value = overridability.find(change.name);
        if (override_value != overridability.end())
            settings.ostr << " " << (override_value->second ? "" : "NOT ") << "OVERRIDABLE";
    }
}

}
