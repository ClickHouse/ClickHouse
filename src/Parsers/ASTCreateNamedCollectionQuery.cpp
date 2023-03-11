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

void ASTCreateNamedCollectionQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("CREATE NAMED COLLECTION ");
    out.writeProbablyBackQuotedIdentifier(collection_name);

    formatOnCluster(out);

    out.writeKeyword(" AS ");
    bool first = true;
    for (const auto & change : changes)
    {
        if (!first)
            out.ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, out.ostr);
        out.ostr << " = ";
        out.writeSecret(applyVisitor(FieldVisitorToString(), change.value));
    }
}

}
