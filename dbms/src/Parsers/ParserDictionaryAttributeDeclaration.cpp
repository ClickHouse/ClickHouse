#include <Parsers/ParserDictionaryAttributeDeclaration.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserDictionaryAttributeDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_parser;
    ParserIdentifierWithOptionalParameters type_parser;
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_expression{"EXPRESSION"};
    ParserKeyword s_hierarchical{"HIERARCHICAL"};
    ParserKeyword s_injective{"INJECTIVE"};
    ParserKeyword s_is_object_id{"IS_OBJECT_ID"};
    ParserLiteral default_parser;
    ParserTernaryOperatorExpression expression_parser;

    /// mandatory column name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;


    /** column name should be followed by type name if it
      *    is not immediately followed by {DEFAULT, MATERIALIZED, ALIAS, COMMENT}
      */
    ASTPtr type;
    ASTPtr default_value;
    ASTPtr expression;
    bool hierarchical = false;
    bool injective = false;
    bool is_object_id = false;

    if (!s_default.check_without_moving(pos, expected) &&
        !s_expression.check_without_moving(pos, expected) &&
        !s_hierarchical.check_without_moving(pos, expected) &&
        !s_injective.check_without_moving(pos, expected) &&
        !s_is_object_id.check_without_moving(pos, expected))
    {
        if (!type_parser.parse(pos, type, expected))
            return false;
    }

    if (s_default.ignore(pos, expected))
        if (!default_parser.parse(pos, default_value, expected))
            return false;

    if (s_expression.ignore(pos, expected))
        if (!expression_parser.parse(pos, expression, expected))
            return false;

    if (s_hierarchical.ignore(pos, expected))
        hierarchical = true;

    if (s_injective.ignore(pos, expected))
        injective = true;

    if (s_is_object_id.ignore(pos, expected))
        is_object_id = true;

    auto attribute_declaration = std::make_shared<ASTDictionaryAttributeDeclaration>();
    node = attribute_declaration;
    tryGetIdentifierNameInto(name, attribute_declaration->name);

    if (type)
    {
        attribute_declaration->type = type;
        attribute_declaration->children.push_back(std::move(type));
    }

    if (default_value)
    {
        attribute_declaration->default_value = default_value;
        attribute_declaration->children.push_back(std::move(default_value));
    }

    if (expression)
    {
        attribute_declaration->expression = expression;
        attribute_declaration->children.push_back(std::move(expression));
    }

    if (hierarchical)
        attribute_declaration->hierarchical = true;

    if (injective)
        attribute_declaration->injective = true;

    if (is_object_id)
        attribute_declaration->is_object_id = true;

    return true;
}


bool ParserDictionaryAttributeDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserDictionaryAttributeDeclaration>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

}
