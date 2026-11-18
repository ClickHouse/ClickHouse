#pragma once


#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query (DESCRIBE | DESC) FILESYSTEM CACHE 'cache_name'
 */
class ParserDescribeCacheQuery : public IParserBase
{
public:
    [[nodiscard]] std::map<String, Documentation> getDocumentation() const override;

protected:
    [[nodiscard]] const char * getName() const override { return "DESCRIBE FILESYSTEM CACHE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
