#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSnapshotQuery.h>

#include <Common/quoteString.h>


namespace DB
{

namespace
{
using Element = ASTSnapshotQuery::Element;
using ElementType = ASTSnapshotQuery::ElementType;

void formatExceptDatabases(const std::set<String> & except_databases, WriteBuffer & ostr, const IAST::FormatSettings &)
{
    if (except_databases.empty())
        return;

    ostr << " EXCEPT " << (except_databases.size() == 1 ? "DATABASE" : "DATABASES") << " ";

    bool need_comma = false;
    for (const auto & database_name : except_databases)
    {
        if (std::exchange(need_comma, true))
            ostr << ",";
        ostr << backQuoteIfNeed(database_name);
    }
}

void formatExceptTables(
    const std::set<DatabaseAndTableName> & except_tables, WriteBuffer & ostr, const IAST::FormatSettings &, bool only_table_names = false)
{
    if (except_tables.empty())
        return;

    ostr << " EXCEPT " << (except_tables.size() == 1 ? "TABLE" : "TABLES") << " ";

    bool need_comma = false;
    for (const auto & table_name : except_tables)
    {
        if (std::exchange(need_comma, true))
            ostr << ", ";

        if (!table_name.first.empty() && !only_table_names)
            ostr << backQuoteIfNeed(table_name.first) << ".";
        ostr << backQuoteIfNeed(table_name.second);
    }
}

void formatElement(const Element & element, WriteBuffer & ostr, const IAST::FormatSettings & format)
{
    switch (element.type)
    {
        case ElementType::TABLE: {
            ostr << "TABLE ";
            if (!element.database_name.empty())
                ostr << backQuoteIfNeed(element.database_name) << ".";
            ostr << backQuoteIfNeed(element.table_name);
            break;
        }
        case ElementType::ALL:
            ostr << "ALL";
            formatExceptDatabases(element.except_databases, ostr, format);
            formatExceptTables(element.except_tables, ostr, format);
            break;
    }
}
}

String ASTSnapshotQuery::getID(char) const
{
    return "SnapshotQuery";
}

ASTPtr ASTSnapshotQuery::clone() const
{
    auto res = make_intrusive<ASTSnapshotQuery>(*this);
    res->children.clear();

    if (snapshot_destination)
        res->set(res->snapshot_destination, snapshot_destination->clone());

    cloneOutputOptions(*res);

    return res;
}

IAST::QueryKind ASTSnapshotQuery::getQueryKind() const
{
    return QueryKind::Snapshot;
}

void ASTSnapshotQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & fs, FormatState &, FormatStateStacked) const
{
    ostr << "SNAPSHOT ";

    formatElement(element, ostr, fs);

    ostr << " TO ";
    snapshot_destination->format(ostr, fs);
}
}
