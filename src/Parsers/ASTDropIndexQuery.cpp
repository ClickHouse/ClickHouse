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

void ASTDropIndexQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);

    out.writeIndent();

    out.writeKeyword("DROP INDEX ");
    out.writeKeyword(if_exists ? "IF EXISTS " : "");
    index_name->formatImpl(out);
    out.writeKeyword(" ON ");

    if (table)
    {
        if (database)
        {
            out.writeIndent();
            out.ostr << backQuoteIfNeed(getDatabase());
            out.ostr << ".";
        }
        out.writeIndent();
        out.ostr << backQuoteIfNeed(getTable());
    }

    formatOnCluster(out);
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
