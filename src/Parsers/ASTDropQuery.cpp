#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + (delim + getDatabase()) + delim + getTable();
    if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + (delim + getDatabase()) + delim + getTable();
    if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + (delim + getDatabase()) + delim + getTable();
    throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = make_intrusive<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDropQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DropQuery");

    w.writeString("database", getDatabase());
    w.writeString("table", getTable());

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    const char * kind_str = "Drop";
    if (kind == Kind::Detach)
        kind_str = "Detach";
    else if (kind == Kind::Truncate)
        kind_str = "Truncate";
    w.writeString("kind", std::string_view(kind_str));

    w.writeBool("if_exists", if_exists);
    w.writeBool("if_empty", if_empty);
    w.writeBool("no_ddl_lock", no_ddl_lock);
    w.writeBool("has_all", has_all);
    w.writeBool("has_tables", has_tables);

    if (!like.empty())
        w.writeString("like", like);

    w.writeBool("not_like", not_like);
    w.writeBool("case_insensitive_like", case_insensitive_like);
    w.writeBool("is_dictionary", is_dictionary);
    w.writeBool("is_view", is_view);
    w.writeBool("sync", sync);
    w.writeBool("permanently", permanently);

    w.writeChild("database_and_tables", database_and_tables);
}

void ASTDropQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    String db = r.getString("database");
    if (!db.empty())
        setDatabase(db);
    String tbl = r.getString("table");
    if (!tbl.empty())
        setTable(tbl);

    cluster = r.getString("cluster");

    String kind_str = r.getString("kind");
    if (kind_str == "Detach")
        kind = Kind::Detach;
    else if (kind_str == "Truncate")
        kind = Kind::Truncate;
    else
        kind = Kind::Drop;

    if_exists = r.getBool("if_exists");
    if_empty = r.getBool("if_empty");
    no_ddl_lock = r.getBool("no_ddl_lock");
    has_all = r.getBool("has_all");
    has_tables = r.getBool("has_tables");

    like = r.getString("like");
    not_like = r.getBool("not_like");
    case_insensitive_like = r.getBool("case_insensitive_like");
    is_dictionary = r.getBool("is_dictionary");
    is_view = r.getBool("is_view");
    sync = r.getBool("sync");
    permanently = r.getBool("permanently");

    auto child = r.readChild("database_and_tables");
    if (child)
    {
        database_and_tables = child;
        children.push_back(database_and_tables);
    }
}

void ASTDropQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        ostr << "TRUNCATE ";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (isTemporary())
        ostr << "TEMPORARY ";

    if (has_all)
        ostr << "ALL ";
    if (has_tables)
        ostr << "TABLES FROM ";
    else if (!table && !database_and_tables && database)
        ostr << "DATABASE ";
    else if (is_dictionary)
        ostr << "DICTIONARY ";
    else if (is_view)
        ostr << "VIEW ";
    else
        ostr << "TABLE ";

    if (if_exists)
        ostr << "IF EXISTS ";

    if (if_empty)
        ostr << "IF EMPTY ";

    if (!table && !database_and_tables && database)
    {
        database->format(ostr, settings, state, frame);
    }
    else if (database_and_tables)
    {
        auto & list = database_and_tables->as<ASTExpressionList &>();
        for (auto it = list.children.begin(); it != list.children.end(); ++it)
        {
            if (it != list.children.begin())
                ostr << ", ";

            auto identifier = dynamic_pointer_cast<ASTTableIdentifier>(*it);
            if (!identifier)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

            if (auto db = identifier->getDatabase())
            {
                db->format(ostr, settings, state, frame);
                ostr << '.';
            }

            auto tb = identifier->getTable();
            chassert(tb);
            tb->format(ostr, settings, state, frame);
        }
    }
    else
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);
    }

    if (!like.empty())
    {
        ostr
            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE")
            << quoteString(like);
    }

    formatOnCluster(ostr, settings);

    if (permanently)
        ostr << " PERMANENTLY";

    if (sync)
        ostr << " SYNC";
}

ASTs ASTDropQuery::getRewrittenASTsOfSingleTable(ASTPtr self) const
{
    ASTs res;
    if (database_and_tables == nullptr)
    {
        res.push_back(self);
        return res;
    }

    auto & list = database_and_tables->as<ASTExpressionList &>();
    for (const auto & child : list.children)
    {
        auto cloned = clone();
        auto & query = cloned->as<ASTDropQuery &>();
        query.database_and_tables = nullptr;
        query.children.clear();

        auto database_and_table = dynamic_pointer_cast<ASTTableIdentifier>(child);
        if (!database_and_table)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

        query.database = database_and_table->getDatabase();
        query.table = database_and_table->getTable();

        if (query.database)
            query.children.push_back(query.database);

        if (query.table)
            query.children.push_back(query.table);

        res.push_back(cloned);
    }
    return res;
}

}
