#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/quoteString.h>

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

    if (database)
    {
        ostr << backQuoteIfNeed(getDatabase());
        ostr << ".";
    }

    ostr << backQuoteIfNeed(getTable());
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
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("assignments", assignments);
    w.writeChild("partition", partition);
    w.writeChild("predicate", predicate);
    w.writeChild("settings_ast", settings_ast);
}

void ASTUpdateQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
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
    assignments = r.readChild("assignments");
    if (!assignments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Update` AST requires 'assignments' during AST JSON deserialization");
    children.push_back(assignments);
    partition = r.readChild("partition");
    if (partition)
        children.push_back(partition);
    predicate = r.readChild("predicate");
    if (!predicate)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Update` AST requires 'predicate' during AST JSON deserialization");
    children.push_back(predicate);
    settings_ast = r.readChild("settings_ast");
    if (settings_ast)
        children.push_back(settings_ast);
}

}
