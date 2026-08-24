#include <Parsers/toOneLineQuery.h>
#include <Parsers/Lexer.h>

namespace DB
{

String toOneLineQuery(const String & query)
{
    String res;
    const char * begin = query.data();
    const char * end = begin + query.size();

    Lexer lexer(begin, end);
    Token token = lexer.nextToken();
    for (; !token.isEnd(); token = lexer.nextToken())
    {
        if (token.type == TokenType::Whitespace)
        {
            res += ' ';
        }
        else if (token.type == TokenType::Comment)
        {
            res.append(token.begin, token.end);
            if (token.end < end && *token.end == '\n')
                res += '\n';
        }
        else
            res.append(token.begin, token.end);
    }

    return res;
}

}
