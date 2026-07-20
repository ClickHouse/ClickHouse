#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

ASTPtr ASTCreateWorkloadQuery::clone() const
{
    auto res = std::make_shared<ASTCreateWorkloadQuery>(*this);
    res->children.clear();

    res->workload_name = workload_name->clone();
    res->children.push_back(res->workload_name);

    if (workload_parent)
    {
        res->workload_parent = workload_parent->clone();
        res->children.push_back(res->workload_parent);
    }

    res->changes = changes;

    return res;
}

void ASTCreateWorkloadQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & format, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "WORKLOAD ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << backQuoteIfNeed(getWorkloadName());

    formatOnCluster(ostr, format);

    if (hasParent())
    {
        ostr << " IN ";
        ostr << backQuoteIfNeed(getWorkloadParent());
    }

    if (!changes.empty())
    {
        ostr << ' ' << "SETTINGS" << ' ';

        bool first = true;

        for (const auto & change : changes)
        {
            if (!first)
                ostr << ", ";
            else
                first = false;
            ostr << change.name << " = " << applyVisitor(FieldVisitorToString(), change.value);
            if (!change.resource.empty())
            {
                ostr << ' ' << "FOR" << ' ';
                ostr << backQuoteIfNeed(change.resource);
            }
        }
    }
}

String ASTCreateWorkloadQuery::getWorkloadName() const
{
    String name;
    tryGetIdentifierNameInto(workload_name, name);
    return name;
}

bool ASTCreateWorkloadQuery::hasParent() const
{
    return workload_parent != nullptr;
}

String ASTCreateWorkloadQuery::getWorkloadParent() const
{
    String name;
    tryGetIdentifierNameInto(workload_parent, name);
    return name;
}

}
