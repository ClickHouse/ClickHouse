#include <iomanip>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
}

String ASTInsertQuery::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTInsertQuery::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTInsertQuery::setDatabase(const String & name)
{
    if (name.empty())
        database.reset();
    else
        database = std::make_shared<ASTIdentifier>(name);
}

void ASTInsertQuery::setTable(const String & name)
{
    if (name.empty())
        table.reset();
    else
        table = std::make_shared<ASTIdentifier>(name);
}

void ASTInsertQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    ostr << (settings.hilite ? hilite_keyword : "") << "INSERT INTO ";
    if (table_function)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "FUNCTION ";
        table_function->formatImpl(ostr, settings, state, frame);
        if (partition_by)
        {
            ostr << " PARTITION BY ";
            partition_by->formatImpl(ostr, settings, state, frame);
        }
    }
    else if (table_id)
    {
        ostr << (settings.hilite ? hilite_none : "")
                      << (!table_id.database_name.empty() ? backQuoteIfNeed(table_id.database_name) + "." : "") << backQuoteIfNeed(table_id.table_name);
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

    if (columns)
    {
        ostr << " (";
        columns->formatImpl(ostr, settings, state, frame);
        ostr << ")";
    }

    if (infile)
    {
        ostr
            << (settings.hilite ? hilite_keyword : "")
            << " FROM INFILE "
            << (settings.hilite ? hilite_none : "")
            << quoteString(infile->as<ASTLiteral &>().value.safeGet<std::string>());
        if (compression)
            ostr
                << (settings.hilite ? hilite_keyword : "")
                << " COMPRESSION "
                << (settings.hilite ? hilite_none : "")
                << quoteString(compression->as<ASTLiteral &>().value.safeGet<std::string>());
    }

    if (settings_ast)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "SETTINGS " << (settings.hilite ? hilite_none : "");
        settings_ast->formatImpl(ostr, settings, state, frame);
    }

    /// Compatibility for INSERT without SETTINGS to format in oneline, i.e.:
    ///
    ///     INSERT INTO foo VALUES
    ///
    /// But
    ///
    ///     INSERT INTO foo
    ///     SETTINGS max_threads=1
    ///     VALUES
    ///
    char delim = settings_ast ? settings.nl_or_ws : ' ';

    if (select)
    {
        ostr << delim;
        select->formatImpl(ostr, settings, state, frame);
    }

    if (!select)
    {
        if (!format.empty())
        {
            ostr << delim
                          << (settings.hilite ? hilite_keyword : "") << "FORMAT " << (settings.hilite ? hilite_none : "") << format;
        }
        else if (!infile)
        {
            ostr << delim
                          << (settings.hilite ? hilite_keyword : "") << "VALUES" << (settings.hilite ? hilite_none : "");
        }
    }
}

void ASTInsertQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(table_id.database_name);
    hash_state.update(table_id.table_name);
    hash_state.update(table_id.uuid);
    hash_state.update(format);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


static void tryFindInputFunctionImpl(const ASTPtr & ast, ASTPtr & input_function)
{
    if (!ast)
        return;
    for (const auto & child : ast->children)
        tryFindInputFunctionImpl(child, input_function);

    if (const auto * table_function_ast = ast->as<ASTFunction>())
    {
        if (table_function_ast->name == "input")
        {
            if (input_function)
                throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "You can use 'input()' function only once per request.");
            input_function = ast;
        }
    }
}


void ASTInsertQuery::tryFindInputFunction(ASTPtr & input_function) const
{
    tryFindInputFunctionImpl(select, input_function);
}

}
