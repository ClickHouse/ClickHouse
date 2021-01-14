#pragma once

#include <BaseErrorListener.h>


namespace DB
{

class ParserErrorListener : public antlr4::BaseErrorListener
{
public:
    void syntaxError(
        antlr4::Recognizer * recognizer,
        antlr4::Token * token,
        size_t line,
        size_t pos,
        const std::string & message,
        std::exception_ptr e) override;
};

}
