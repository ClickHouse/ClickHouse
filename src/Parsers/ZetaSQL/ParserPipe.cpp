#include "Parsers/ZetaSQL/ParserPipe.h"
#include "Parsers/CommonParsers.h"
#include "Parsers/Lexer.h"
#include "Parsers/ZetaSQL/ParserWhereClause.h"

namespace DB::ZetaSQL
{
    bool ParserPipe::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ParserWhereClause where_parser;
        if(!ParserToken(TokenType::PipeOperator).ignore(pos, expected))
            return false;

        return where_parser.parse(pos, node, expected);
    }
}
