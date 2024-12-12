#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTDropIndexQuery.h>


namespace DB
{

/** Get the text that identifies this element. */
String ASTDropIndexQuery::getID(char delim) const
{
    return "DropIndexQuery" + (delim + getDatabase()) + delim + getTable();
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

void ASTDropIndexQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

    ostr << "DROP INDEX " << (if_exists ? "IF EXISTS " : "");
    index_name->formatImpl(ostr, settings, state, frame);
    ostr << " ON ";

    ostr << (settings.hilite ? hilite_none : "");

    if (table)
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
}

ASTPtr ASTDropIndexQuery::convertToASTAlterCommand() const
{
    auto command = std::make_shared<ASTAlterCommand>();

    command->type = ASTAlterCommand::DROP_INDEX;
    command->if_exists = if_exists;

    command->index = command->children.emplace_back(index_name).get();

    return command;
}

}
