#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTAlterQuery.h>


namespace DB
{

/** Get the text that identifies this element. */
String ASTCreateIndexQuery::getID(char delim) const
{
    return "CreateIndexQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTCreateIndexQuery::clone() const
{
    auto res = std::make_shared<ASTCreateIndexQuery>(*this);
    res->children.clear();

    res->index_name = index_name->clone();
    res->children.push_back(res->index_name);

    res->index_decl = index_decl->clone();
    res->children.push_back(res->index_decl);

    cloneTableOptions(*res);

    return res;
}

void ASTCreateIndexQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

    ostr << "CREATE " << (unique ? "UNIQUE " : "") << "INDEX " << (if_not_exists ? "IF NOT EXISTS " : "");
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

    ostr << " ";

    index_decl->formatImpl(ostr, settings, state, frame);
}

ASTPtr ASTCreateIndexQuery::convertToASTAlterCommand() const
{
    auto command = std::make_shared<ASTAlterCommand>();

    command->type = ASTAlterCommand::ADD_INDEX;
    command->if_not_exists = if_not_exists;

    command->index = command->children.emplace_back(index_name).get();
    command->index_decl = command->children.emplace_back(index_decl).get();

    return command;
}

}
