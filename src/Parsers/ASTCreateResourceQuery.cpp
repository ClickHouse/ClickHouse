#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

ASTPtr ASTCreateResourceQuery::clone() const
{
    auto res = std::make_shared<ASTCreateResourceQuery>(*this);
    res->children.clear();

    res->resource_name = resource_name->clone();
    res->children.push_back(res->resource_name);

    res->operations = operations;

    return res;
}

void ASTCreateResourceQuery::formatImpl(const IAST::FormatSettings & format, IAST::FormatState &, IAST::FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        format.ostr << "OR REPLACE ";

    format.ostr << "RESOURCE ";

    if (if_not_exists)
        format.ostr << "IF NOT EXISTS ";

    format.ostr << (format.hilite ? hilite_none : "");

    format.ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(getResourceName()) << (format.hilite ? hilite_none : "");

    formatOnCluster(format);

    format.ostr << " (";

    bool first = true;
    for (const auto & operation : operations)
    {
        if (!first)
            format.ostr << ", ";
        else
            first = false;

        switch (operation.mode)
        {
            case AccessMode::Read:
            {
                format.ostr << (format.hilite ? hilite_keyword : "") << "READ ";
                break;
            }
            case AccessMode::Write:
            {
                format.ostr << (format.hilite ? hilite_keyword : "") << "WRITE ";
                break;
            }
        }
        if (operation.disk)
        {
            format.ostr << "DISK " << (format.hilite ? hilite_none : "");
            format.ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(*operation.disk) << (format.hilite ? hilite_none : "");
        }
        else
            format.ostr << "ANY DISK" << (format.hilite ? hilite_none : "");
    }

    format.ostr << ")";
}

String ASTCreateResourceQuery::getResourceName() const
{
    String name;
    tryGetIdentifierNameInto(resource_name, name);
    return name;
}

}
