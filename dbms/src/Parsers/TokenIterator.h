#pragma once

#include <vector>
#include <Parsers/Lexer.h>


namespace DB
{

/** Parser operates on lazy stream of tokens.
  * It could do lookaheads of any depth.
  */

/** Used as an input for parsers.
  * All whitespace and comment tokens are transparently skipped.
  */
class Tokens
{
private:
    std::vector<Token> data;
    Lexer lexer;

public:
    Tokens(const char * begin, const char * end) : lexer(begin, end) {}

    const Token & operator[] (size_t index)
    {
        while (true)
        {
            if (index < data.size())
                return data[index];

            if (!data.empty() && data.back().type == TokenType::EndOfStream)
                return data.back();

            Token token = lexer.nextToken();

            if (token.type != TokenType::Whitespace
                && token.type != TokenType::Comment)
                data.emplace_back(token);
        }
    }
};


/// To represent position in a token stream.
class TokenIterator
{
private:
    Tokens * tokens;
    size_t index = 0;

public:
    TokenIterator(Tokens & tokens) : tokens(&tokens) {}

    const Token & get() { return (*tokens)[index]; }
    const Token & operator*() { return get(); }
    const Token * operator->() { return &get(); }

    TokenIterator & operator++() { ++index; return *this; }
    TokenIterator & operator--() { --index; return *this; }

    bool operator< (const TokenIterator & rhs) const { return index < rhs.index; }
    bool operator== (const TokenIterator & rhs) const { return index == rhs.index; }

    bool isValid() { return get().type < TokenType::EndOfStream; }
};

}
