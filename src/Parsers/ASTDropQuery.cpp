#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
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
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDropQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "");
    if (kind == ASTDropQuery::Kind::Drop)
        ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        ostr << "TRUNCATE ";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (temporary)
        ostr << "TEMPORARY ";

    if (has_all_tables)
        ostr << "ALL TABLES FROM ";
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

    ostr << (settings.hilite ? hilite_none : "");

    if (!table && !database_and_tables && database)
    {
        database->formatImpl(ostr, settings, state, frame);
    }
    else if (database_and_tables)
    {
        auto & list = database_and_tables->as<ASTExpressionList &>();
        for (auto * it = list.children.begin(); it != list.children.end(); ++it)
        {
            if (it != list.children.begin())
                ostr << ", ";

            auto identifier = dynamic_pointer_cast<ASTTableIdentifier>(*it);
            if (!identifier)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

            if (auto db = identifier->getDatabase())
            {
                db->formatImpl(ostr, settings, state, frame);
                ostr << '.';
            }

            auto tb = identifier->getTable();
            chassert(tb);
            tb->formatImpl(ostr, settings, state, frame);
        }
    }
    else
    {
        if (database)
        {
            database->formatImpl(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->formatImpl(ostr, settings, state, frame);
    }

    formatOnCluster(ostr, settings);

    if (permanently)
        ostr << " PERMANENTLY";

    if (sync)
        ostr << (settings.hilite ? hilite_keyword : "") << " SYNC" << (settings.hilite ? hilite_none : "");
}

ASTs ASTDropQuery::getRewrittenASTsOfSingleTable()
{
    ASTs res;
    if (database_and_tables == nullptr)
    {
        res.push_back(shared_from_this());
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
