#include <iomanip>
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


void ASTInsertQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "INSERT INTO ";
    if (table_function)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FUNCTION ";
        table_function->formatImpl(settings, state, frame);
        if (partition_by)
        {
            settings.ostr << " PARTITION BY ";
            partition_by->formatImpl(settings, state, frame);
        }
    }
    else
        settings.ostr << (settings.hilite ? hilite_none : "")
                      << (!table_id.database_name.empty() ? backQuoteIfNeed(table_id.database_name) + "." : "") << backQuoteIfNeed(table_id.table_name);

    if (columns)
    {
        settings.ostr << " (";
        columns->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }

    if (select)
    {
        settings.ostr << " ";
        select->formatImpl(settings, state, frame);
    }
    else if (watch)
    {
        settings.ostr << " ";
        watch->formatImpl(settings, state, frame);
    }
    else
    {
        if (infile)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM INFILE " << (settings.hilite ? hilite_none : "") << infile->as<ASTLiteral &>().value.safeGet<std::string>();
            if (compression)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " COMPRESSION " << (settings.hilite ? hilite_none : "") << compression->as<ASTLiteral &>().value.safeGet<std::string>();
        }
        if (!format.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FORMAT " << (settings.hilite ? hilite_none : "") << format;
        }
        else if (!infile)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " VALUES" << (settings.hilite ? hilite_none : "");
        }
    }

    if (settings_ast)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "SETTINGS " << (settings.hilite ? hilite_none : "");
        settings_ast->formatImpl(settings, state, frame);
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
                throw Exception("You can use 'input()' function only once per request.", ErrorCodes::INVALID_USAGE_OF_INPUT);
            input_function = ast;
        }
    }
}


void ASTInsertQuery::tryFindInputFunction(ASTPtr & input_function) const
{
    tryFindInputFunctionImpl(select, input_function);
}

}
