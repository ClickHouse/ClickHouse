#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String ASTUpdateQuery::getID(char delim) const
{
    return "UpdateQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTUpdateQuery::clone() const
{
    auto res = make_intrusive<ASTUpdateQuery>(*this);
    res->children.clear();

    const auto add_children_if_needed = [&](const auto & src, auto & dst)
    {
        if (!src)
            return;

        dst = src->clone();
        res->children.push_back(dst);
    };

    add_children_if_needed(partition, res->partition);
    add_children_if_needed(predicate, res->predicate);
    add_children_if_needed(assignments, res->assignments);
    add_children_if_needed(settings_ast, res->settings_ast);

    cloneTableOptions(*res);
    return res;
}

void ASTUpdateQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "UPDATE ";

    /// Format the target as AST nodes (not as strings via `getDatabase`/`getTable`),
    /// because the table can be parameterized (e.g. `UPDATE {tbl:Identifier}`), and
    /// a query parameter is not representable as a string identifier.
    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << ".";
    }

    chassert(table);
    table->format(ostr, settings, state, frame);
    formatOnCluster(ostr, settings);

    ostr << " SET ";
    assignments->format(ostr, settings, state, frame);

    if (partition)
    {
        ostr << " IN PARTITION ";
        partition->format(ostr, settings, state, frame);
    }

    ostr << " WHERE ";
    predicate->format(ostr, settings, state, frame);
}

void ASTUpdateQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "UpdateQuery");
    if (!cluster.empty())
        w.writeString("cluster", cluster);
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("assignments", assignments);
    w.writeChild("partition", partition);
    w.writeChild("predicate", predicate);
    /// `UPDATE` is parsed by `ParserUpdateQuery`, not `ParserQueryWithOutput`, so the only
    /// supported output-suffix clause is the query-local `SETTINGS`. Do not serialize the
    /// full output options (`INTO OUTFILE` / `FORMAT` / `COMPRESSION`), which the SQL parser
    /// can never produce for this query and which would break the JSON round-trip contract.
    w.writeChild("settings_ast", settings_ast);
}

void ASTUpdateQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    cluster = r.getString("cluster");
    database = r.readChild("database");
    if (database)
        children.push_back(database);
    /// `table`, `assignments`, and `predicate` are required: `formatQueryImpl` dereferences
    /// `assignments` and `predicate` unconditionally and always formats the table name. Reject
    /// malformed JSON that omits them instead of producing an AST that crashes later.
    table = r.readChild("table");
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Update` AST requires 'table' during AST JSON deserialization");
    children.push_back(table);
    /// `assignments` is parser-produced as an `ASTExpressionList` whose children are all
    /// `ASTAssignment` (`ParserUpdateQuery` builds it with a `ParserList` of `ParserAssignment`).
    /// `MutationCommand::parse` downcasts every child with `->as<ASTAssignment &>()`, so a foreign
    /// node type or child type from malformed `clickhouse_json` must be rejected here instead of
    /// reaching that downcast.
    auto assignments_list = r.readChildOfType<ASTExpressionList>("assignments");
    if (!assignments_list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Update` AST requires 'assignments' (as an expression list) during AST JSON deserialization");
    for (const auto & assignment : assignments_list->children)
        if (!assignment || !assignment->as<ASTAssignment>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Every child of 'assignments' must be an `ASTAssignment` during AST JSON deserialization");
    assignments = assignments_list;
    children.push_back(assignments);
    partition = r.readChild("partition");
    if (partition)
        children.push_back(partition);
    predicate = r.readChild("predicate");
    if (!predicate)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Update` AST requires 'predicate' during AST JSON deserialization");
    children.push_back(predicate);
    /// Only the query-local `SETTINGS` clause is supported here (see `writeJSON`).
    settings_ast = r.readChildOfType<ASTSetQuery>("settings_ast");
    if (settings_ast)
        children.push_back(settings_ast);
}

}
