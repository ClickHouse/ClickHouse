#include <Common/quoteString.h>
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

    // TODO(serxa): clone settings

    return res;
}

void ASTCreateWorkloadQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "WORKLOAD ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getWorkloadName()) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);

    if (hasParent())
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN " << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getWorkloadParent()) << (settings.hilite ? hilite_none : "");
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
