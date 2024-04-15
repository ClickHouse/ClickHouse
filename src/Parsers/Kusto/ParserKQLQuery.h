#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Kusto/IKQLParserBase.h>

namespace DB
{
class ParserKQLBase : public IKQLParserBase
{
public:
    static String getExprFromToken(KQLPos & pos);
    static String getExprFromToken(const String & text, uint32_t max_depth, uint32_t max_backtracks);
    static String getExprFromPipe(KQLPos & pos);
    static bool setSubQuerySource(ASTPtr & select_query, ASTPtr & source, bool dest_is_subquery, bool src_is_subquery);
    static bool parseSQLQueryByString(ParserPtr && parser, String & query, ASTPtr & select_node, uint32_t max_depth, uint32_t max_backtracks);
    bool parseByString(String expr, ASTPtr & node, uint32_t max_depth, uint32_t max_backtracks);
};

class ParserKQLQuery : public IKQLParserBase
{
protected:
    static std::unique_ptr<IKQLParserBase> getOperator(String & op_name);
    const char * getName() const override { return "KQL query"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};

class ParserKQLSubquery : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL subquery"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};

class ParserSimpleCHSubquery : public ParserKQLBase
{
public:
    explicit ParserSimpleCHSubquery(ASTPtr parent_select_node_ = nullptr) { parent_select_node = parent_select_node_; }

protected:
    const char * getName() const override { return "Simple ClickHouse subquery"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
    ASTPtr parent_select_node;
};

class BracketCount
{
public:
    void count(IKQLParser::KQLPos & pos)
    {
        if (pos->type == KQLTokenType::OpeningRoundBracket)
            ++round_bracket_count;
        if (pos->type == KQLTokenType::ClosingRoundBracket)
            --round_bracket_count;
        if (pos->type == KQLTokenType::OpeningSquareBracket)
            ++square_bracket_count;
        if (pos->type == KQLTokenType::ClosingSquareBracket)
            --square_bracket_count;
    }
    bool isZero() const { return round_bracket_count == 0 && square_bracket_count == 0; }

private:
    int16_t round_bracket_count = 0;
    int16_t square_bracket_count = 0;
};
}
