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

void ASTCreateResourceQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & format, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << (format.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "RESOURCE ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << (format.hilite ? hilite_none : "");

    ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(getResourceName()) << (format.hilite ? hilite_none : "");

    formatOnCluster(ostr, format);

    ostr << " (";

    bool first = true;
    for (const auto & operation : operations)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        if (operation.mode == ResourceAccessMode::MasterThread)
        {
            ostr << (format.hilite ? hilite_keyword : "") << "MASTER THREAD" << (format.hilite ? hilite_none : "");
        }
        else if (operation.mode == ResourceAccessMode::WorkerThread)
        {
            ostr << (format.hilite ? hilite_keyword : "") << "WORKER THREAD" << (format.hilite ? hilite_none : "");
        }
        else if (operation.mode == ResourceAccessMode::Query)
        {
            ostr << (format.hilite ? hilite_keyword : "") << "QUERY" << (format.hilite ? hilite_none : "");
        }
        else
        {
            switch (operation.mode)
            {
                case ResourceAccessMode::DiskRead:
                {
                    ostr << (format.hilite ? hilite_keyword : "") << "READ ";
                    break;
                }
                case ResourceAccessMode::DiskWrite:
                {
                    ostr << (format.hilite ? hilite_keyword : "") << "WRITE ";
                    break;
                }
                default:
                    chassert(false);
            }
            if (operation.disk)
            {
                ostr << "DISK " << (format.hilite ? hilite_none : "");
                ostr << (format.hilite ? hilite_identifier : "") << backQuoteIfNeed(*operation.disk) << (format.hilite ? hilite_none : "");
            }
            else
                ostr << "ANY DISK" << (format.hilite ? hilite_none : "");
        }
    }

    ostr << ")";
}

String ASTCreateResourceQuery::getResourceName() const
{
    String name;
    tryGetIdentifierNameInto(resource_name, name);
    return name;
}

}
