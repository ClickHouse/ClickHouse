#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTOptimizeQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "OPTIMIZE TABLE ";

    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << '.';
    }

    chassert(table);
    table->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    if (partition)
    {
        ostr << " PARTITION ";
        partition->format(ostr, settings, state, frame);
    }

    if (dry_run)
    {
        ostr << " DRY RUN";
        if (parts_list)
        {
            ostr << " PARTS ";
            parts_list->format(ostr, settings, state, frame);
        }
    }

    if (final)
        ostr << " FINAL";

    if (deduplicate)
        ostr << " DEDUPLICATE";

    if (cleanup)
        ostr << " CLEANUP";

    if (deduplicate_by_columns)
    {
        ostr << " BY ";
        deduplicate_by_columns->format(ostr, settings, state, frame);
    }
}

void ASTOptimizeQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "OptimizeQuery");
    if (!cluster.empty())
        w.writeString("cluster", cluster);
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("partition", partition);
    if (final)
        w.writeBool("final", true);
    if (deduplicate)
        w.writeBool("deduplicate", true);
    if (cleanup)
        w.writeBool("cleanup", true);
    if (dry_run)
        w.writeBool("dry_run", true);
    w.writeChild("deduplicate_by_columns", deduplicate_by_columns);
    w.writeChild("parts_list", parts_list);
}

void ASTOptimizeQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    cluster = r.getString("cluster");
    database = r.readChild("database");
    if (database)
        children.push_back(database);
    table = r.readChild("table");
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'table' field in `OptimizeQuery` during AST JSON deserialization");
    children.push_back(table);
    partition = r.readChild("partition");
    if (partition)
        children.push_back(partition);
    final = r.getBool("final");
    deduplicate = r.getBool("deduplicate");
    cleanup = r.getBool("cleanup");
    dry_run = r.getBool("dry_run");
    deduplicate_by_columns = r.readChild("deduplicate_by_columns");
    if (deduplicate_by_columns)
        children.push_back(deduplicate_by_columns);
    parts_list = r.readChild("parts_list");
    if (parts_list)
        children.push_back(parts_list);
}

}
