#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Grammar/Grammar.h>


using namespace DB;
using namespace Grammar;

int main(int, char **)
{
    auto select             = Keyword("SELECT");
    auto from               = Keyword("FROM");
    auto where              = Keyword("WHERE");
    auto group              = Keyword("GROUP");
    auto by                 = Keyword("BY");
    auto having             = Keyword("HAVING");
    auto order              = Keyword("ORDER");
    auto limit              = Keyword("LIMIT");
    auto offset             = Keyword("OFFSET");
    auto asc                = Keyword("ASC");
    auto ascending          = Keyword("ASCENDING");
    auto desc               = Keyword("DESC");
    auto descending         = Keyword("DESCENDING");
    auto collate            = Keyword("COLLATE");
    auto is                 = Keyword("IS");
    auto null               = Keyword("NULL");
    auto global             = Keyword("GLOBAL");

    auto interval           = Keyword("INTERVAL");
    auto second             = Keyword("SECOND");
    auto minute             = Keyword("MINUTE");
    auto hour               = Keyword("HOUR");
    auto day                = Keyword("DAY");
    auto week               = Keyword("WEEK");
    auto month              = Keyword("MONTH");
    auto year               = Keyword("YEAR");

    auto op_and             = Keyword("AND");
    auto op_or              = Keyword("OR");
    auto op_not             = Keyword("NOT");
    auto op_between         = Keyword("BETWEEN");
    auto op_like            = Keyword("LIKE");
    auto op_in              = Keyword("IN");
    auto op_not_like        = Sequence(op_not, op_like);
    auto op_not_in          = Sequence(op_not, op_in);
    auto op_global_in       = Sequence(global, op_in);
    auto op_global_not_in   = Sequence(global, op_not, op_in);

    auto is_null            = Sequence(is, null);
    auto is_not_null        = Sequence(is, op_not, null);

    auto order_direction    = Tag(NodeType::OrderDirection, Alternative(asc, ascending, desc, descending));

    auto group_by           = Sequence(group, by);
    auto order_by           = Sequence(order, by);

    auto comma              = Just<TokenType::Comma>();
    auto dot                = Just<TokenType::Dot>();
    auto lparen             = Just<TokenType::OpeningRoundBracket>();
    auto rparen             = Just<TokenType::ClosingRoundBracket>();
    auto word               = Just<TokenType::BareWord>();
    auto quoted_identifier  = Just<TokenType::QuotedIdentifier>();
    auto string_literal     = Just<TokenType::StringLiteral>();
    auto number             = Just<TokenType::Number>();
    auto open_square_bracket = Just<TokenType::OpeningSquareBracket>();
    auto close_square_bracket = Just<TokenType::ClosingSquareBracket>();

    Indirect expression_list;
    Indirect simple_expression;
    Indirect expression;

    auto literal            = Tag(NodeType::Literal, Alternative(number, string_literal));
    auto identifier         = Tag(NodeType::Identifier, Alternative(word, quoted_identifier));
    auto compound_identifier = Tag(NodeType::CompoundIdentifier, DelimitedSequence(identifier, dot));

    auto asterisk           = Just<TokenType::Asterisk>();
    auto plus               = Just<TokenType::Plus>();
    auto minus              = Just<TokenType::Minus>();
    auto slash              = Just<TokenType::Slash>();
    auto percent            = Just<TokenType::Percent>();
    auto arrow              = Just<TokenType::Arrow>();
    auto question           = Just<TokenType::QuestionMark>();
    auto colon              = Just<TokenType::Colon>();
    auto equals             = Just<TokenType::Equals>();
    auto not_equals         = Just<TokenType::NotEquals>();
    auto less               = Just<TokenType::Less>();
    auto greater            = Just<TokenType::Greater>();
    auto less_or_equals     = Just<TokenType::LessOrEquals>();
    auto greater_or_equals  = Just<TokenType::GreaterOrEquals>();
    auto concatenation      = Just<TokenType::Concatenation>();

    auto qualified_asterisk = Sequence(compound_identifier, dot, asterisk);

    Indirect array_subscript_op;
    Indirect tuple_subscript_op;
    Indirect unary_minus_op;
    Indirect multiplicative_op;
    Indirect interval_op;
    Indirect additive_op;
    Indirect concat_op;
    Indirect between_op;
    Indirect comparison_op;
    Indirect null_check_op;
    Indirect logical_not_op;
    Indirect logical_and_op;
    Indirect logical_or_op;
    Indirect conditional_op;
    Indirect lambda_op;

    array_subscript_op = Alternative(Tag(NodeType::ArraySubscript,
        Sequence(simple_expression, open_square_bracket, expression, close_square_bracket)), simple_expression);
    tuple_subscript_op = Alternative(Tag(NodeType::TupleSubscript,
        Sequence(array_subscript_op, dot, array_subscript_op)), array_subscript_op);
    unary_minus_op     = Alternative(Tag(NodeType::UnaryMinus,
        Sequence(minus, tuple_subscript_op)), tuple_subscript_op);
    multiplicative_op  = Alternative(Tag(NodeType::Multiplicative,
        ProperDelimitedSequence(unary_minus_op, Alternative(asterisk, slash, percent))), unary_minus_op);
    interval_op        = Alternative(Tag(NodeType::Interval,
        Sequence(interval, multiplicative_op, Alternative(second, minute, hour, day, week, month, year))), multiplicative_op);
    additive_op        = Alternative(Tag(NodeType::Additive,
        ProperDelimitedSequence(interval_op, Alternative(plus, minus))), interval_op);
    concat_op          = Alternative(Tag(NodeType::Concat,
        ProperDelimitedSequence(additive_op, concatenation)), additive_op);
    between_op         = Alternative(Tag(NodeType::Between,
        Sequence(concat_op, op_between, concat_op, op_and, concat_op)), concat_op);
    comparison_op      = Alternative(Tag(NodeType::Comparison,
        Sequence(between_op,
            Alternative(
                equals, not_equals, greater, less, greater_or_equals, less_or_equals,
                op_like, op_not_like, op_in, op_not_in, op_global_in, op_global_not_in),
            between_op)), between_op);
    null_check_op      = Alternative(Tag(NodeType::NullCheck, Sequence(comparison_op, Alternative(is_null, is_not_null))), comparison_op);
    logical_not_op     = Alternative(Tag(NodeType::LogicalNot, Sequence(op_not, null_check_op)), null_check_op);
    logical_and_op     = Alternative(Tag(NodeType::LogicalAnd, ProperDelimitedSequence(logical_not_op, op_and)), logical_not_op);
    logical_or_op      = Alternative(Tag(NodeType::LogicalOr, ProperDelimitedSequence(logical_and_op, op_or)), logical_and_op);
    conditional_op     = Alternative(Tag(NodeType::Conditional, Sequence(logical_or_op, question, logical_or_op, colon, logical_or_op)), logical_or_op);
    lambda_op          = Alternative(Tag(NodeType::Lambda, Sequence(conditional_op, arrow, conditional_op)), conditional_op);

    expression              = lambda_op;

    auto paren_expr         = Sequence(lparen, expression_list, rparen);
    auto function           = Tag(NodeType::Function, Sequence(identifier, paren_expr));

    simple_expression       = Alternative(literal, function, compound_identifier, asterisk, qualified_asterisk, paren_expr);

    auto collate_modifier   = Tag(NodeType::CollateModifier, Sequence(collate, string_literal));
    auto order_expression   = Tag(NodeType::OrderExpression, Sequence(expression, order_direction, Optional(collate_modifier)));
    auto order_expression_list = Tag(NodeType::OrderExpressionList, DelimitedSequence(order_expression, comma));

    auto limit_one_number   = number;
    auto limit_two_numbers  = Sequence(number, comma, number);
    auto limit_offset       = Sequence(number, offset, number);

    auto limit_expression   = Tag(NodeType::LimitExpression, Alternative(limit_one_number, limit_two_numbers, limit_offset));
    auto limit_by_expression = Sequence(limit, number, by, expression);

    expression_list         = Tag(NodeType::ExpressionList, DelimitedSequence(expression, comma));

    auto select_query       = Tag(NodeType::SelectQuery,
        Sequence(select, expression_list,
            Optional(Sequence(from, compound_identifier)),
            Optional(Sequence(where, expression)),
            Optional(Sequence(group_by, expression_list)),
            Optional(Sequence(having, expression_list)),
            Optional(Sequence(order_by, order_expression_list)),
            Optional(limit_by_expression),
            Optional(Sequence(limit, limit_expression))))
    ;

    auto parser = select_query;

    String query;
    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    readStringUntilEOF(query, in);

    Tokens tokens(query.data(), query.data() + query.size());
    TokenIterator token_iterator(tokens);

    Node node;
    parser.match(token_iterator, node);

    node.print();

    return 0;
}

