#include <iomanip>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void ASTInsertQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "INSERT INTO ";
    if (table_function)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FUNCTION ";
        table_function->formatImpl(settings, state, frame);
    }
    else
        settings.ostr << (settings.hilite ? hilite_none : "")
                      << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

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
    else
    {
        if (!format.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FORMAT " << (settings.hilite ? hilite_none : "") << format;
        }
        else
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " VALUES" << (settings.hilite ? hilite_none : "");
        }
    }

    if (settings_ast)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SETTINGS " << (settings.hilite ? hilite_none : "");
        settings_ast->formatImpl(settings, state, frame);
    }
}


void ASTInsertQuery::tryFindInputFunction(const ASTPtr & ast, ASTPtr & input_function) const
{
    if (!ast)
        return;
    for (const auto & child : ast->children)
        tryFindInputFunction(child, input_function);

    if (const auto * table_function = ast->as<ASTFunction>())
    {
        if (table_function->name == "input")
        {
            if (input_function)
                throw Exception("You can use 'input()' function only once per request.", ErrorCodes::LOGICAL_ERROR);
            input_function = ast;
        }
    }
}

}
