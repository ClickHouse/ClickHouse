#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserUnionQueryElement : public IParserBase
{
public:
    explicit ParserUnionQueryElement(bool allow_query_parameters_ = false)
        : allow_query_parameters(allow_query_parameters_)
    {
    }

    bool allow_query_parameters;

protected:
    const char * getName() const override { return "SELECT query, subquery, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
