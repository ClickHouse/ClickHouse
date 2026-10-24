#include <Common/quoteString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

ASTPtr ASTCreateResourceQuery::clone() const
{
    auto res = make_intrusive<ASTCreateResourceQuery>(*this);
    res->children.clear();

    res->resource_name = resource_name->clone();
    res->children.push_back(res->resource_name);

    res->operations = operations;

    return res;
}

void ASTCreateResourceQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives
    /// the format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(or_replace);
    hash_state.update(if_not_exists);
    hash_state.update(cluster);

    hash_state.update(operations.size());
    for (const auto & operation : operations)
    {
        hash_state.update(operation.mode);
        /// `disk` is `std::optional<String>`: fold presence and content explicitly, a raw
        /// `update(operation.disk)` would hash the object representation.
        hash_state.update(operation.disk.has_value());
        if (operation.disk)
            hash_state.update(*operation.disk);
    }
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
        else if (operation.mode == ResourceAccessMode::MemoryReservation)
        {
            ostr << "MEMORY RESERVATION";
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
