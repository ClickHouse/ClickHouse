#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>

namespace DB
{

bool ParserPartition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_id("ID");
    ParserStringLiteral parser_string_literal;
    ParserExpression parser_expr;

    Pos begin = pos;

    auto partition = std::make_shared<ASTPartition>();

    if (s_id.ignore(pos, expected))
    {
        ASTPtr partition_id;
        if (!parser_string_literal.parse(pos, partition_id, expected))
            return false;

        partition->id = partition_id->as<ASTLiteral &>().value.get<String>();
    }
    else
    {
        ASTPtr value;
        if (!parser_expr.parse(pos, value, expected))
            return false;

        size_t fields_count;
        String fields_str;

        const auto * tuple_ast = value->as<ASTFunction>();
        bool surrounded_by_parens = false;
        if (tuple_ast && tuple_ast->name == "tuple")
        {
            surrounded_by_parens = true;
            const auto * arguments_ast = tuple_ast->arguments->as<ASTExpressionList>();
            if (arguments_ast)
                fields_count = arguments_ast->children.size();
            else
                fields_count = 0;
        }
        else if (const auto * literal = value->as<ASTLiteral>())
        {
            if (literal->value.getType() == Field::Types::Tuple)
            {
                surrounded_by_parens = true;
                fields_count = literal->value.get<const Tuple &>().size();
            }
            else
            {
                fields_count = 1;
                fields_str = String(begin->begin, pos->begin - begin->begin);
            }
        }
        else
            return false;

        if (surrounded_by_parens)
        {
            Pos left_paren = begin;
            Pos right_paren = pos;

            while (left_paren != right_paren && left_paren->type != TokenType::OpeningRoundBracket)
                ++left_paren;
            if (left_paren->type != TokenType::OpeningRoundBracket)
                return false;

            while (right_paren != left_paren && right_paren->type != TokenType::ClosingRoundBracket)
                --right_paren;
            if (right_paren->type != TokenType::ClosingRoundBracket)
                return false;

            fields_str = String(left_paren->end, right_paren->begin - left_paren->end);
        }

        partition->value = value;
        partition->children.push_back(value);
        partition->fields_str = std::move(fields_str);
        partition->fields_count = fields_count;
    }

    node = partition;
    return true;
}

}
