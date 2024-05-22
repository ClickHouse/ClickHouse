#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>

namespace DB
{
class ParserKQLBase : public IParserBase
{
public:
    static String getExprFromToken(Pos & pos);
    static String getExprFromToken(const String & text, uint32_t max_depth, uint32_t max_backtracks);
    static String getExprFromPipe(Pos & pos);
    static bool setSubQuerySource(ASTPtr & select_query, ASTPtr & source, bool dest_is_subquery, bool src_is_subquery);
    static bool parseSQLQueryByString(ParserPtr && parser, String & query, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks);
    bool parseByString(String expr, ASTPtr & node, uint32_t max_depth, uint32_t max_backtracks);
};

class ParserKQLQuery : public IParserBase
{
protected:
    static std::unique_ptr<IParserBase> getOperator(String & op_name);
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserKQLSubquery : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserSimpleCHSubquery : public ParserKQLBase
{
public:
    explicit ParserSimpleCHSubquery(ASTPtr parent_select_node_ = nullptr) { parent_select_node = parent_select_node_; }

protected:
    const char * getName() const override { return "Simple ClickHouse subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    ASTPtr parent_select_node;
};

class BracketCount
{
public:
    void count(IParser::Pos & pos)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++round_bracket_count;
        if (pos->type == TokenType::ClosingRoundBracket)
            --round_bracket_count;
        if (pos->type == TokenType::OpeningSquareBracket)
            ++square_bracket_count;
        if (pos->type == TokenType::ClosingSquareBracket)
            --square_bracket_count;
    }
    bool isZero() const { return round_bracket_count == 0 && square_bracket_count == 0; }

private:
    int16_t round_bracket_count = 0;
    int16_t square_bracket_count = 0;
};
}
