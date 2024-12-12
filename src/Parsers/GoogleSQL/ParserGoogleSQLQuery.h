#pragma once

#include <memory>
#include <Parsers/IParserBase.h>
#include "Parsers/GoogleSQL/ASTPipelineQuery.h"
#include "Parsers/IAST_fwd.h"

namespace DB::GoogleSQL
{
class ParserGoogleSQLQuery final : public IParserBase
{
public:
    const char * getName() const override { return "PRQL Statement"; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    size_t max_query_size;
    size_t max_parser_depth;
    size_t max_parser_backtracks;
    ASTPtr translateToClickHouseAST(std::shared_ptr<ASTPipelineQuery> & query_ast);
};
}
