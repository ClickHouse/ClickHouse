#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTQueryParameter.h>

namespace DB
{

bool ParserPartition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_id("ID");
    ParserKeyword s_all("ALL");
    ParserStringLiteral parser_string_literal;
    ParserSubstitution parser_substitution;
    ParserLiteral literal_parser;
    ParserFunction function_parser(false, false);


    Pos begin = pos;

    auto partition = std::make_shared<ASTPartition>();

    if (s_id.ignore(pos, expected))
    {
        ASTPtr partition_id;
        if (!parser_string_literal.parse(pos, partition_id, expected) && !parser_substitution.parse(pos, partition_id, expected))
            return false;

        if (auto * partition_id_literal = partition_id->as<ASTLiteral>(); partition_id_literal != nullptr)
            partition->setPartitionID(partition_id);
        else if (auto * partition_id_query_parameter = partition_id->as<ASTQueryParameter>(); partition_id_query_parameter != nullptr)
            partition->setPartitionID(partition_id);
        else
            return false;
    }
    else if (s_all.ignore(pos, expected))
    {
        partition->all = true;
    }
    else
    {
        bool surrounded_by_parens = false;
        ASTPtr value;
        std::optional<size_t> fields_count;
        if (literal_parser.parse(pos, value, expected))
        {
            auto * literal = value->as<ASTLiteral>();
            if (literal->value.getType() == Field::Types::Tuple)
            {
                surrounded_by_parens = true;
                fields_count = literal->value.get<const Tuple &>().size();
            }
            else
            {
                fields_count = 1;
            }
        }
        else if (function_parser.parse(pos, value, expected))
        {
            const auto * tuple_ast = value->as<ASTFunction>();
            if (tuple_ast)
            {
                if (tuple_ast->name == "tuple")
                {
                    surrounded_by_parens = true;
                    const auto * arguments_ast = tuple_ast->arguments->as<ASTExpressionList>();
                    if (arguments_ast)
                        fields_count = arguments_ast->children.size();
                    else
                        fields_count = 0;
                }
            }
        }
        else if (!parser_substitution.parse(pos, value, expected))
        {
            return false;
        }

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
        }

        partition->setPartitionValue(value);
        partition->fields_count = fields_count;
    }

    node = partition;
    return true;
}

}
