#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
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
    writeOutputOptionsJSON(w);
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
    /// `parts_list` is produced by the parser only for `OPTIMIZE ... DRY RUN PARTS '...'`:
    /// a non-empty `ASTExpressionList` of string literals. `formatImpl` prints it only inside
    /// the `dry_run` branch and `InterpreterOptimizeQuery` downcasts each entry to `ASTLiteral`
    /// unconditionally. Reject any parser-impossible shape so `clickhouse_json` cannot, e.g.,
    /// attach a part list to a non-dry-run OPTIMIZE (which formatting silently drops while
    /// execution ignores it) or reach a logical error through the unconditional downcast.
    parts_list = r.readChildOfType<ASTExpressionList>("parts_list");
    if (parts_list)
    {
        if (!dry_run)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'parts_list' is only valid for OPTIMIZE ... DRY RUN PARTS during AST JSON deserialization");
        if (parts_list->children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'parts_list' must be a non-empty list of part names during AST JSON deserialization");
        for (const auto & part : parts_list->children)
        {
            const auto * literal = part ? part->as<ASTLiteral>() : nullptr;
            if (!literal || literal->value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Each entry of 'parts_list' must be a string literal during AST JSON deserialization");
        }
        children.push_back(parts_list);
    }
    else if (dry_run)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "OPTIMIZE ... DRY RUN requires a non-empty 'parts_list' during AST JSON deserialization");
    readOutputOptionsJSON(r);
}

}
