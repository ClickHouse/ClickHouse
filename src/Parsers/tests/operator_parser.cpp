#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Grammar/Grammar.h>


using namespace DB;
using namespace Grammar;

int main(int, char **)
{
    /*
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
    */

    Indirect operator_expression;

    auto lparen             = Just<TokenType::OpeningRoundBracket>();
    auto rparen             = Just<TokenType::ClosingRoundBracket>();
    auto dot                = Just<TokenType::Dot>();

    auto word               = Just<TokenType::BareWord>();
    auto quoted_identifier  = Just<TokenType::QuotedIdentifier>();
    auto string_literal     = Just<TokenType::StringLiteral>();
    auto number             = Just<TokenType::Number>();

    auto literal            = Tag(NodeType::Literal, Alternative(number, string_literal));
    auto identifier         = Tag(NodeType::Identifier, Alternative(word, quoted_identifier));
    auto compound_identifier = Tag(NodeType::CompoundIdentifier, DelimitedSequence(identifier, dot));

    auto paren_expr         = Sequence(lparen, operator_expression, rparen);
    auto function           = Tag(NodeType::Function, Sequence(identifier, lparen, Optional(operator_expression), rparen));

    auto asterisk           = Just<TokenType::Asterisk>();
    auto qualified_asterisk = Sequence(compound_identifier, dot, asterisk);

    auto term = Alternative(literal, function, compound_identifier, asterisk, qualified_asterisk, paren_expr);

    Operators operators
    {
        {1, OperatorType::InfixSuffix, NodeType::ArraySubscript, Just<TokenType::OpeningSquareBracket>(), Just<TokenType::ClosingSquareBracket>()},
        {2, OperatorType::Infix, NodeType::TupleSubscript, Just<TokenType::Dot>()},
        {3, OperatorType::Prefix, NodeType::UnaryMinus, Just<TokenType::Minus>()},
        {4, OperatorType::Infix, NodeType::Multiplicative, Just<TokenType::Asterisk>()},
        {4, OperatorType::Infix, NodeType::Multiplicative, Just<TokenType::Slash>()},
        {4, OperatorType::Infix, NodeType::Multiplicative, Just<TokenType::Percent>()},
        /// Interval operator
        {6, OperatorType::Infix, NodeType::Additive, Just<TokenType::Plus>()},
        {6, OperatorType::Infix, NodeType::Additive, Just<TokenType::Minus>()},
        {7, OperatorType::Infix, NodeType::Concat, Just<TokenType::Concatenation>()},
        /// Between operator
        /// ...
    };

    operator_expression = OperatorExpression(term, operators);

    String query;
    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    readStringUntilEOF(query, in);

    Tokens tokens(query.data(), query.data() + query.size());
    TokenIterator token_iterator(tokens);

    Node node;
    operator_expression.match(token_iterator, node);

    node.print();

    return 0;
}


