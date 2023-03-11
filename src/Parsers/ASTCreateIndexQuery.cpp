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

void ASTCreateIndexQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);

    out.writeIndent();

    out.writeKeyword("CREATE INDEX ");
    out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
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

    if (!cluster.empty())
        out.ostr << " ";

    index_decl->formatImpl(out);
}

ASTPtr ASTCreateIndexQuery::convertToASTAlterCommand() const
{
    auto command = std::make_shared<ASTAlterCommand>();
    command->type = ASTAlterCommand::ADD_INDEX;
    command->index = index_name->clone();
    command->index_decl = index_decl->clone();
    command->if_not_exists = if_not_exists;

    return command;
}

}
