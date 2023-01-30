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

void ASTInsertQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.writeKeyword("INSERT INTO ");
    if (table_function)
    {
        settings.writeKeyword("FUNCTION ");
        table_function->formatImpl(settings, state, frame);
        if (partition_by)
        {
            settings.writeKeyword(" PARTITION BY ");
            partition_by->formatImpl(settings, state, frame);
        }
    }
    else if (table_id)
    {
        settings.ostr << (!table_id.database_name.empty() ? backQuoteIfNeed(table_id.database_name) + "." : "") << backQuoteIfNeed(table_id.table_name);
    }
    else
    {
        settings.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());
    }

    if (columns)
    {
        settings.ostr << " (";
        columns->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }

    if (infile)
    {
        settings.writeKeyword(" FROM INFILE ");
        settings.ostr << quoteString(infile->as<ASTLiteral &>().value.safeGet<std::string>());
        if (compression)
        {
            settings.writeKeyword(" COMPRESSION ");
            settings.ostr << quoteString(compression->as<ASTLiteral &>().value.safeGet<std::string>());
        }
    }

    if (settings_ast)
    {
        settings.nlOrWs();
        settings.writeKeyword("SETTINGS ");
        settings_ast->formatImpl(settings, state, frame);
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
    if (select)
    {
        if (settings_ast)
            settings.nlOrWs();
        else
            settings.ostr << ' ';
        select->formatImpl(settings, state, frame);
    }
    else if (watch)
    {
        if (settings_ast)
            settings.nlOrWs();
        else
            settings.ostr << ' ';
        watch->formatImpl(settings, state, frame);
    }

    if (!select && !watch)
    {
        if (!format.empty())
        {
            if (settings_ast)
                settings.nlOrWs();
            else
                settings.ostr << ' ';
            settings.writeKeyword("FORMAT ");
            settings.ostr << format;
        }
        else if (!infile)
        {
            if (settings_ast)
                settings.nlOrWs();
            else
                settings.ostr << ' ';
            settings.writeKeyword("VALUES");
        }
    }
}

void ASTInsertQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(table_id.database_name);
    hash_state.update(table_id.table_name);
    hash_state.update(table_id.uuid);
    hash_state.update(format);
    IAST::updateTreeHashImpl(hash_state);
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
