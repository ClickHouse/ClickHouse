#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


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
    res->children.clear();
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDropQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    auto db = getDatabaseAst();
    auto tbl = getTableAst();
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
    else if (!tbl && !database_and_tables && db)
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

    if (!tbl && !database_and_tables && db)
    {
        db->format(ostr, settings, state, frame);
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

            if (auto idb = identifier->getDatabase())
            {
                idb->format(ostr, settings, state, frame);
                ostr << '.';
            }

            auto tb = identifier->getTable();
            chassert(tb);
            tb->format(ostr, settings, state, frame);
        }
    }
    else
    {
        if (db)
        {
            db->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(tbl);
        tbl->format(ostr, settings, state, frame);
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

        query.setDatabaseAst(database_and_table->getDatabase());
        query.setTableAst(database_and_table->getTable());

        res.push_back(cloned);
    }
    return res;
}

}
