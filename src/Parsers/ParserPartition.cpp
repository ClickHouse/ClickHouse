#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTQueryParameter.h>


namespace DB
{

bool ParserPartition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_id(Keyword::ID);
    ParserKeyword s_all(Keyword::ALL);
    ParserStringLiteral parser_string_literal;
    ParserSubstitution parser_substitution;
    ParserExpression parser_expr;

    auto partition = make_intrusive<ASTPartition>();

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
        ASTPtr value;
        std::optional<size_t> fields_count;
        if (parser_substitution.parse(pos, value, expected))
        {
            /// It can be tuple substitution
            fields_count = std::nullopt;
        }
        else if (parser_expr.parse(pos, value, expected))
        {
            if (const auto * function_ast = value->as<ASTFunction>(); function_ast)
            {
                if (function_ast->name == "tuple")
                {
                    const auto * arguments_ast = function_ast->arguments->as<ASTExpressionList>();
                    if (arguments_ast)
                        fields_count = arguments_ast->children.size();
                    else
                        fields_count = 0;
                }
                else if (isFunctionCast(function_ast))
                {
                    /// A cast of a literal or of a tuple, e.g. `_CAST(20260624, 'UInt32')`.
                    /// Query parameter substitution (`PARTITION {param:Type}`) rewrites the
                    /// parameter into this form, and the result must be parseable back, e.g.
                    /// when mutation commands are re-read from ZooKeeper or disk. Leave
                    /// `fields_count` unset, the same as for an unsubstituted parameter; it is
                    /// deduced from the cast operand in `MergeTreeData::getPartitionIDFromQuery`.
                    if (!function_ast->arguments || function_ast->arguments->children.size() != 2)
                        return false;

                    const auto & cast_operand = function_ast->arguments->children.at(0);
                    const auto * inner_function = cast_operand->as<ASTFunction>();
                    bool is_tuple_function = inner_function && inner_function->name == "tuple";
                    if (!is_tuple_function && !cast_operand->as<ASTLiteral>())
                        return false;
                }
                else
                {
                    return false;
                }
            }
            else if (const auto * literal_ast = value->as<ASTLiteral>(); literal_ast)
            {
                if (literal_ast->value.getType() == Field::Types::Tuple)
                {
                    fields_count = literal_ast->value.safeGet<Tuple>().size();
                }
                else
                {
                    fields_count = 1;
                }
            }
            else
                return false;
        }
        else
        {
            return false;
        }

        partition->setPartitionValue(value);
        partition->fields_count = fields_count;
    }

    node = partition;
    return true;
}

}
