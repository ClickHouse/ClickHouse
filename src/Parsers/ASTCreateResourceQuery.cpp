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
    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "RESOURCE ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << backQuoteIfNeed(getResourceName());

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
            ostr << "MASTER THREAD";
        }
        else if (operation.mode == ResourceAccessMode::WorkerThread)
        {
            ostr << "WORKER THREAD";
        }
        else if (operation.mode == ResourceAccessMode::Query)
        {
            ostr << "QUERY";
        }
        else
        {
            switch (operation.mode)
            {
                case ResourceAccessMode::DiskRead:
                {
                    ostr << "READ ";
                    break;
                }
                case ResourceAccessMode::DiskWrite:
                {
                    ostr << "WRITE ";
                    break;
                }
                default:
                    chassert(false);
            }
            if (operation.disk)
            {
                ostr << "DISK ";
                ostr << backQuoteIfNeed(*operation.disk);
            }
            else
                ostr << "ANY DISK";
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
