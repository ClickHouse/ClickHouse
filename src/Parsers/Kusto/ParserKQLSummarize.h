#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{
class ParserKQLSummarize : public ParserKQLBase
{
public:
    ASTPtr group_expression_list;
    ASTPtr tables;
    ASTPtr where_expression;

    void setTableName(String table_name_) {table_name = table_name_;}
    void setFilterPos(std::vector<Pos> &filter_pos_) {filter_pos = filter_pos_;}
protected:
    const char * getName() const override { return "KQL summarize"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    static std::pair<String, String> removeLastWord(String input);
    static String getBinGroupbyString(String expr_bin);
private:
    String table_name;
    std::vector<Pos> filter_pos;
};

}
