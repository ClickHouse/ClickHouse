#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** Get the text that identifies this element. */
String ASTDropIndexQuery::getID(char delim) const
{
    return "DropIndexQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDropIndexQuery::clone() const
{
    auto res = make_intrusive<ASTDropIndexQuery>(*this);
    res->children.clear();

    res->index_name = index_name->clone();
    res->children.push_back(res->index_name);

    cloneTableOptions(*res);

    return res;
}

void ASTDropIndexQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DropIndexQuery");

    w.writeBool("if_exists", if_exists);

    /// The plain database/table names. These are sufficient for ordinary
    /// (non-parameterized) identifiers.
    w.writeString("database", getDatabase());
    w.writeString("table", getTable());

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    /// Serialize the index name AST (required by `formatQueryImpl`).
    w.writeChild("index_name", index_name);

    /// Serialize the database/table identifier ASTs as well, so that parameterized
    /// names like `{tbl:Identifier}` survive the round-trip. The plain `database`/`table`
    /// strings above cannot represent query parameters.
    w.writeChild("database_ast", database);
    w.writeChild("table_ast", table);
}

void ASTDropIndexQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    if_exists = r.getBool("if_exists");

    cluster = r.getString("cluster");

    index_name = r.readChild("index_name");
    if (!index_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`DropIndexQuery` must specify 'index_name' during AST JSON deserialization");
    children.push_back(index_name);

    /// Prefer the parameterized identifier ASTs (which can represent query parameters);
    /// otherwise fall back to the plain string names. `setDatabase`/`setTable` register
    /// the created identifier in `children`, so do not push again on that path.
    if (auto database_child = r.readChild("database_ast"))
    {
        database = database_child;
        children.push_back(database);
    }
    else
        setDatabase(r.getString("database"));

    if (auto table_child = r.readChild("table_ast"))
    {
        table = table_child;
        children.push_back(table);
    }
    else
        setTable(r.getString("table"));
}

void ASTDropIndexQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    ostr << indent_str;

    ostr << "DROP INDEX " << (if_exists ? "IF EXISTS " : "");
    index_name->format(ostr, settings, state, frame);
    ostr << " ON ";

    if (table)
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);
    }

    formatOnCluster(ostr, settings);
}

ASTPtr ASTDropIndexQuery::convertToASTAlterCommand() const
{
    auto command = make_intrusive<ASTAlterCommand>();

    command->type = ASTAlterCommand::DROP_INDEX;
    command->if_exists = if_exists;

    command->index = command->children.emplace_back(index_name).get();

    return command;
}

}
