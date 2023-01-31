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
    settings.writeKeyword("CREATE NAMED COLLECTION ");
    settings.writeProbablyBackQuotedIdentifier(collection_name);

    formatOnCluster(settings);

    settings.writeKeyword(" AS ");
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

}
