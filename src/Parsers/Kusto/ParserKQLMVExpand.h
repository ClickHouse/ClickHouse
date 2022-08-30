
#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLMVExpand : public ParserKQLBase
{
public:

    void setTableName(String table_name_) {table_name = table_name_;}
    ASTPtr settings;
protected:
    static std::unordered_map<String,String> type_cast;

    struct ColumnArrayExpr {
        String alias;
        String column_array_expr;
        String to_type;
        ColumnArrayExpr(String alias_, String column_array_expr_, String to_type_)
        :alias(alias_), column_array_expr(column_array_expr_), to_type(to_type_){}
    };
    using ColumnArrayExprs = std::vector<ColumnArrayExpr>;

    struct KQLMVExpand {
        ColumnArrayExprs column_array_exprs;
        String bagexpansion;
        String with_itemindex;
        String limit;
    };
 
    bool parseColumnArrayExprs(ColumnArrayExprs & column_array_exprs, Pos & pos, Expected & expected);
    bool parserMVExpand(KQLMVExpand & kql_mv_expand, Pos & pos, Expected & expected);
    String genQuery(KQLMVExpand & kql_mv_expand, String input);
    const char * getName() const override { return "KQL mv-expand"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    String table_name;
};
}



