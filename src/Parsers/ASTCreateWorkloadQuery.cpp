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
    ostr << (format.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "WORKLOAD ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << (format.hilite ? hilite_none : "");

    ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(getWorkloadName()) << (format.hilite ? hilite_none : "");

    formatOnCluster(ostr, format);

    if (hasParent())
    {
        ostr << (format.hilite ? hilite_keyword : "") << " IN " << (format.hilite ? hilite_none : "");
        ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(getWorkloadParent()) << (format.hilite ? hilite_none : "");
    }

    if (!changes.empty())
    {
        ostr << ' ' << (format.hilite ? hilite_keyword : "") << "SETTINGS" << (format.hilite ? hilite_none : "") << ' ';

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
                ostr << ' ' << (format.hilite ? hilite_keyword : "") << "FOR" << (format.hilite ? hilite_none : "") << ' ';
                ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(change.resource) << (format.hilite ? hilite_none : "");
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
