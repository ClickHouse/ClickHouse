#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTDropIndexQuery.h>


namespace DB
{

/** Get the text that identifies this element. */
String ASTDropIndexQuery::getID(char delim) const
{
    return "CreateIndexQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDropIndexQuery::clone() const
{
    auto res = std::make_shared<ASTDropIndexQuery>(*this);
    res->children.clear();

    res->index_name = index_name->clone();
    res->children.push_back(res->index_name);

    cloneTableOptions(*res);

    return res;
}

void ASTDropIndexQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

    settings.ostr << "DROP INDEX " << (if_exists ? "IF EXISTS " : "");
    index_name->formatImpl(settings, state, frame);
    settings.ostr << " ON ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table)
    {
        if (database)
        {
            settings.ostr << indent_str << backQuoteIfNeed(getDatabase());
            settings.ostr << ".";
        }
        settings.ostr << indent_str << backQuoteIfNeed(getTable());
    }

    formatOnCluster(settings);
}

ASTPtr ASTDropIndexQuery::convertToASTAlterCommand() const
{
    auto command = std::make_shared<ASTAlterCommand>();
    command->type = ASTAlterCommand::DROP_INDEX;
    command->index = index_name->clone();
    command->if_exists = if_exists;

    return command;
}

}
