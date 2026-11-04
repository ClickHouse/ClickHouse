#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses both CREATE HANDLER and ALTER HANDLER queries, producing an ASTCreateHandlerQuery.
class ParserCreateHandlerQuery : public IParserBase
{
public:
    explicit ParserCreateHandlerQuery(const char * end_) : end(end_) {}

protected:
    const char * getName() const override { return "CREATE HANDLER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    const char * end;
};

}
