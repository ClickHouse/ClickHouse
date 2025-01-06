#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/formatSettingName.h>

namespace DB
{

ASTPtr ASTAlterNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTAlterNamedCollectionQuery>(*this);
}

void ASTAlterNamedCollectionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "ALTER NAMED COLLECTION ";
    if (if_exists)
        settings.ostr << "IF EXISTS ";
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(collection_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
    if (!changes.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " SET " << (settings.hilite ? hilite_none : "");
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
    if (!delete_keys.empty())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " DELETE " << (settings.hilite ? hilite_none : "");
        bool first = true;
        for (const auto & key : delete_keys)
        {
            if (!first)
                settings.ostr << ", ";
            else
                first = false;

            formatSettingName(key, settings.ostr);
        }
    }
}

}
