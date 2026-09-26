#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTCheckTableQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CheckTableQuery");
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("partition", partition);
    if (!part_name.empty())
        w.writeString("part_name", part_name);
    writeOutputOptionsJSON(w);
}

void ASTCheckTableQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    /// `database`/`table` are parser-produced identifiers; `getDatabase`/`getTable` extract a name via
    /// `tryGetIdentifierNameInto` (returning empty for non-identifiers), so a non-identifier node would
    /// format one target while execution resolves a different/empty one. Reject it at the boundary.
    database = r.readChildOfType<ASTIdentifier>("database");
    if (database)
        children.push_back(database);
    table = r.readChildOfType<ASTIdentifier>("table");
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing required 'table' in `CheckTableQuery` during AST JSON deserialization");
    children.push_back(table);
    /// `partition` is parser-produced as an `ASTPartition`; `InterpreterCheckQuery` forwards it into
    /// `StorageMergeTree::getCheckTaskList` / `StorageReplicatedMergeTree::getCheckTaskList`, which
    /// downcast via `partition->as<ASTPartition>()` and raise `LOGICAL_ERROR` otherwise. Reject any
    /// other node type at the deserialization boundary instead.
    partition = r.readChildOfType<ASTPartition>("partition");
    /// `partition` is deliberately member-only: `ParserCheckQuery` only puts database and table in
    /// `children`. Keeping that shape is necessary for the JSON round trip to preserve the tree hash.
    part_name = r.getString("part_name");
    /// The parser produces either `PARTITION <expr>` or `PART '<name>'`, never both
    /// (`getPartitionOrPartitionID` returns only `partition` and ignores `part_name`). A JSON AST
    /// carrying both would format as two clauses while executing against the partition only, so
    /// reject this parser-impossible shape at the boundary.
    if (partition && !part_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`CheckTableQuery` cannot carry both 'partition' and 'part_name' during AST JSON deserialization");
    readOutputOptionsJSON(r);
}

void ASTCheckAllTablesQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CheckAllTablesQuery");
    writeOutputOptionsJSON(w);
}

void ASTCheckAllTablesQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    readOutputOptionsJSON(r);
}

}
