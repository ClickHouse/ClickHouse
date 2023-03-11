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

void ASTAlterNamedCollectionQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("Alter NAMED COLLECTION ");
    out.writeProbablyBackQuotedIdentifier(collection_name);
    formatOnCluster(out);
    if (!changes.empty())
    {
        out.writeKeyword(" SET ");
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
    if (!delete_keys.empty())
    {
        out.writeKeyword(" DELETE ");
        bool first = true;
        for (const auto & key : delete_keys)
        {
            if (!first)
                out.ostr << ", ";
            else
                first = false;

            formatSettingName(key, out.ostr);
        }
    }
}

}
