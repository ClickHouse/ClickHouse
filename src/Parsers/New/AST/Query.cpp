#include <Parsers/New/AST/Query.h>


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

}
