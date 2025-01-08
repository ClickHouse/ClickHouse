#include "Parsers/IAST_fwd.h"
#include "Parsers/Lexer.h"
#include "Parsers/ZetaSQL/ASTZetaSQLQuery.h"
#include "Parsers/ZetaSQL/ZetaSQLTranslator.h"
#include "Parsers/ZetaSQL/ParserZetaSQLQuery.h"
#include "Parsers/ZetaSQL/ParserFromStatement.h"
#include "Parsers/ZetaSQL/ParserPipe.h"

namespace DB::ZetaSQL {
    bool ParserZetaSQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ParserFromStatement from_parser;
        ParserPipe pipe_parser;
        Translator translator;
        auto ast = std::make_shared<ASTZetaSQLQuery>();
        node = ast;

        if(!from_parser.parse(pos, node, expected))
            return false;

        while(pipe_parser.parse(pos, node, expected));

        if (!pos->isEnd() && pos->type != TokenType::Semicolon)
            return false;

        node = translator.translateAST(*ast);
        return true;
    }
}
