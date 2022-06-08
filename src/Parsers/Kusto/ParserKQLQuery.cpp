#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTable.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/Kusto/ParserKQLFilter.h>
#include <Parsers/Kusto/ParserKQLSort.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/Kusto/ParserKQLFilter.h>
#include <Parsers/Kusto/ParserKQLLimit.h>

namespace DB
{

bool ParserKQLBase :: parsePrepare(Pos & pos)
{
   op_pos.push_back(pos);
   return true;
}

String ParserKQLBase :: getExprFromToken(Pos pos)
{
    String res;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark)
    {
        res = res + String(pos->begin,pos->end) +" ";
        ++pos;
    }
    return res;
}

bool ParserKQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;

    ParserKQLFilter KQLfilter_p;
    ParserKQLLimit KQLlimit_p;
    ParserKQLProject KQLproject_p;
    ParserKQLSort KQLsort_p;
    ParserKQLSummarize KQLsummarize_p;
    ParserKQLTable KQLtable_p;

    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr where_expression;
    ASTPtr group_expression_list;
    ASTPtr order_expression_list;
    ASTPtr limit_length;

    std::unordered_map<std::string, ParserKQLBase * > KQLParser = {
        { "filter",&KQLfilter_p},
        { "where",&KQLfilter_p},
        { "limit",&KQLlimit_p},
        { "take",&KQLlimit_p},
        { "project",&KQLproject_p},
        { "sort",&KQLsort_p},
        { "order",&KQLsort_p},
        { "summarize",&KQLsummarize_p},
        { "table",&KQLtable_p}
    };

    std::vector<std::pair<String, Pos>> operation_pos;

    operation_pos.push_back(std::make_pair("table",pos));

    while (!pos->isEnd())
    {
        ++pos;
        if (pos->type == TokenType::PipeMark)
        {
            ++pos;
            String KQLoperator(pos->begin,pos->end);
            if (pos->type != TokenType::BareWord || KQLParser.find(KQLoperator) == KQLParser.end())
                return false;
            ++pos;
            operation_pos.push_back(std::make_pair(KQLoperator,pos));
        }
    }

    for (auto &op_pos : operation_pos)
    {
        auto KQLoperator = op_pos.first;
        auto npos = op_pos.second;
        if (!npos.isValid())
            return false;

        if (!KQLParser[KQLoperator]->parsePrepare(npos))
            return false;
    }

    if (!KQLtable_p.parse(pos, tables, expected))
        return false;

    if (!KQLproject_p.parse(pos, select_expression_list, expected))
        return false;

    if (!KQLlimit_p.parse(pos, limit_length, expected))
        return false;

    if (!KQLfilter_p.parse(pos, where_expression, expected))
        return false;

    if (!KQLsort_p.parse(pos, order_expression_list, expected))
         return false;

    if (!KQLsummarize_p.parse(pos, select_expression_list, expected))
         return false;
    else
        group_expression_list = KQLsummarize_p.group_expression_list;

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));

    return true;
}

}
