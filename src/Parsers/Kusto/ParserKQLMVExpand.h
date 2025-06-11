#pragma once

#include <Parsers/Kusto/IKQLParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLMVExpand : public ParserKQLBase
{
protected:
    static std::unordered_map<String, String> type_cast;

    struct ColumnArrayExpr
    {
        String alias;
        String column_array_expr;
        String to_type;
        ColumnArrayExpr(String alias_, String column_array_expr_, String to_type_)
            : alias(alias_), column_array_expr(column_array_expr_), to_type(to_type_)
        {
        }
    };
    using ColumnArrayExprs = std::vector<ColumnArrayExpr>;

    struct KQLMVExpand
    {
        ColumnArrayExprs column_array_exprs;
        String bagexpansion;
        String with_itemindex;
        String limit;
    };

    static bool parseColumnArrayExprs(ColumnArrayExprs & column_array_exprs, KQLPos & pos, KQLExpected & expected);
    static bool parserMVExpand(KQLMVExpand & kql_mv_expand, KQLPos & pos, KQLExpected & expected);
    static bool genQuery(KQLMVExpand & kql_mv_expand, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks);

    const char * getName() const override { return "KQL mv-expand"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};
}
