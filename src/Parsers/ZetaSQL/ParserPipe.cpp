#include "Parsers/ZetaSQL/ParserPipe.h"
#include "Parsers/CommonParsers.h"
#include "Parsers/Lexer.h"
#include "Parsers/ZetaSQL/ParserWhereClause.h"
#include "Parsers/ZetaSQL/ParserAggregateStatement.h"

namespace DB::ZetaSQL
{
    bool ParserPipe::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ParserWhereClause where_parser;
        ParserAggregateStatement aggregate_parser;
        if(!ParserToken(TokenType::PipeOperator).ignore(pos, expected))
            return false;

        if(ParserKeyword(Keyword::WHERE).ignore(pos, expected))
            return where_parser.parse(pos, node, expected);
        else if(ParserKeyword(Keyword::AGGREGATE).ignore(pos, expected))
            return aggregate_parser.parse(pos, node, expected);
        else
            return false;
    }
}
