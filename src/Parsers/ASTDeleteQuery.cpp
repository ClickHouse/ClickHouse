#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

String ASTDeleteQuery::getID(char delim) const
{
    return "DeleteQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDeleteQuery::clone() const
{
    auto res = make_intrusive<ASTDeleteQuery>(*this);
    res->children.clear();

    if (partition)
    {
        res->partition = partition->clone();
        res->children.push_back(res->partition);
    }

    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }

    if (settings_ast)
    {
        res->settings_ast = settings_ast->clone();
        res->children.push_back(res->settings_ast);
    }

    cloneTableOptions(*res);
    return res;
}

void ASTDeleteQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "DELETE FROM ";

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
        ostr << " IN PARTITION ";
        partition->format(ostr, settings, state, frame);
    }

    ostr << " WHERE ";
    predicate->format(ostr, settings, state, frame);
}

void ASTDeleteQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DeleteQuery");
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("partition", partition);
    w.writeChild("predicate", predicate);
    w.writeChild("settings_ast", settings_ast);
}

void ASTDeleteQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    database = r.readChild("database");
    if (database)
        children.push_back(database);
    table = r.readChild("table");
    if (table)
        children.push_back(table);
    partition = r.readChild("partition");
    if (partition)
        children.push_back(partition);
    predicate = r.readChild("predicate");
    if (predicate)
        children.push_back(predicate);
    settings_ast = r.readChild("settings_ast");
    if (settings_ast)
        children.push_back(settings_ast);
}

}
