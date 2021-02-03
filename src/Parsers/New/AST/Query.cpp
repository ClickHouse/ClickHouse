#include <Parsers/New/AST/Query.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>


namespace DB::AST
{

void Query::setOutFile(PtrTo<StringLiteral> literal)
{
    out_file = literal;
}

void Query::setFormat(PtrTo<Identifier> id)
{
    format = id;
}

void Query::convertToOldPartially(const std::shared_ptr<ASTQueryWithOutput> & query) const
{
    if (out_file)
    {
        query->out_file = out_file->convertToOld();
        query->children.push_back(query->out_file);
    }
    if (format)
    {
        query->format = format->convertToOld();
        query->children.push_back(query->format);
    }
}

}
