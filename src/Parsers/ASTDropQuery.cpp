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

void ASTDropQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    if (kind == ASTDropQuery::Kind::Drop)
        settings.ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        settings.ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        settings.ostr << "TRUNCATE ";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (temporary)
        settings.ostr << "TEMPORARY ";

    if (has_all_tables)
        settings.ostr << "ALL TABLES FROM ";
    else if (!table && !database_and_tables && database)
        settings.ostr << "DATABASE ";
    else if (is_dictionary)
        settings.ostr << "DICTIONARY ";
    else if (is_view)
        settings.ostr << "VIEW ";
    else
        settings.ostr << "TABLE ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    if (if_empty)
        settings.ostr << "IF EMPTY ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (!table && !database_and_tables && database)
    {
        database->formatImpl(settings, state, frame);
    }
    else if (database_and_tables)
    {
        auto & list = database_and_tables->as<ASTExpressionList &>();
        for (auto * it = list.children.begin(); it != list.children.end(); ++it)
        {
            if (it != list.children.begin())
                settings.ostr << ", ";

            auto identifier = dynamic_pointer_cast<ASTTableIdentifier>(*it);
            if (!identifier)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

            if (auto db = identifier->getDatabase())
            {
                db->formatImpl(settings, state, frame);
                settings.ostr << '.';
            }

            auto tb = identifier->getTable();
            chassert(tb);
            tb->formatImpl(settings, state, frame);
        }
    }
    else
    {
        if (database)
        {
            database->formatImpl(settings, state, frame);
            settings.ostr << '.';
        }

        chassert(table);
        table->formatImpl(settings, state, frame);
    }

    formatOnCluster(settings);

    if (permanently)
        settings.ostr << " PERMANENTLY";

    if (sync)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " SYNC" << (settings.hilite ? hilite_none : "");
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
