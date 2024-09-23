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

    if (workload_parent)
    {
        res->workload_parent = workload_parent->clone();
        res->children.push_back(res->workload_parent);
    }

    if (settings)
    {
        res->settings = settings->clone();
        res->children.push_back(res->settings);
    }

    return res;
}

void ASTCreateWorkloadQuery::formatImpl(const IAST::FormatSettings & format_settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    format_settings.ostr << (format_settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        format_settings.ostr << "OR REPLACE ";

    format_settings.ostr << "WORKLOAD ";

    if (if_not_exists)
        format_settings.ostr << "IF NOT EXISTS ";

    format_settings.ostr << (format_settings.hilite ? hilite_none : "");

    format_settings.ostr << (format_settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getWorkloadName()) << (format_settings.hilite ? hilite_none : "");

    formatOnCluster(format_settings);

    if (hasParent())
    {
        format_settings.ostr << (format_settings.hilite ? hilite_keyword : "") << " IN " << (format_settings.hilite ? hilite_none : "");
        format_settings.ostr << (format_settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getWorkloadParent()) << (format_settings.hilite ? hilite_none : "");
    }

    if (settings)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "SETTINGS" << (format_settings.hilite ? hilite_none : "") << ' ';
        settings->format(format_settings);
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
