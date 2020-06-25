#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

class ParserQuery : public IParserBase
{
protected:
    const char * getName() const override { return "MySQL Query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

ASTPtr tryParseMySQLQuery(const std::string & query, size_t max_query_size, size_t max_parser_depth);

}
