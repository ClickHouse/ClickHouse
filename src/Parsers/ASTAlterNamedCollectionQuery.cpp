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
    settings.writeKeyword("Alter NAMED COLLECTION ");
    settings.writeProbablyBackQuotedIdentifier(collection_name);
    formatOnCluster(settings);
    if (!changes.empty())
    {
        settings.writeKeyword(" SET ");
        bool first = true;
        for (const auto & change : changes)
        {
            if (!first)
                settings.ostr << ", ";
            else
                first = false;

            formatSettingName(change.name, settings.ostr);
            settings.ostr << " = ";
            settings.writeSecret(applyVisitor(FieldVisitorToString(), change.value));
        }
    }
    if (!delete_keys.empty())
    {
        settings.writeKeyword(" DELETE ");
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
