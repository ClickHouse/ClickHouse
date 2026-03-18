#include <Parsers/ParserDictionaryAttributeDeclaration.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserDataType.h>

namespace DB
{

bool ParserDictionaryAttributeDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_parser;
    ParserDataType type_parser;
    ParserKeyword s_default{Keyword::DEFAULT};
    ParserKeyword s_expression{Keyword::EXPRESSION};
    ParserKeyword s_hierarchical{Keyword::HIERARCHICAL};
    ParserKeyword s_bidirectional{Keyword::BIDIRECTIONAL};
    ParserKeyword s_injective{Keyword::INJECTIVE};
    ParserKeyword s_is_object_id{Keyword::IS_OBJECT_ID};
    ParserLiteral default_parser;
    ParserArrayOfLiterals array_literals_parser;
    ParserExpression expression_parser;

    /// mandatory attribute name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    ASTPtr type;
    ASTPtr default_value;
    ASTPtr expression;
    bool hierarchical = false;
    bool bidirectional = false;
    bool injective = false;
    bool is_object_id = false;

    /// attribute name should be followed by type name if it
    if (!type_parser.parse(pos, type, expected))
        return false;

    /// loop to avoid strict order of attribute properties
    while (true)
    {
        if (!default_value && s_default.ignore(pos, expected))
        {
            if (!default_parser.parse(pos, default_value, expected) &&
                !array_literals_parser.parse(pos, default_value, expected))
                return false;

            continue;
        }

        if (!expression && s_expression.ignore(pos, expected))
        {
            if (!expression_parser.parse(pos, expression, expected))
                return false;
            continue;
        }

        /// just single keyword, we don't use "true" or "1" for value
        if (!hierarchical && s_hierarchical.ignore(pos, expected))
        {
            hierarchical = true;
            continue;
        }

        if (!bidirectional && s_bidirectional.ignore(pos, expected))
        {
            bidirectional = true;
            continue;
        }

        if (!injective && s_injective.ignore(pos, expected))
        {
            injective = true;
            continue;
        }

        if (!is_object_id && s_is_object_id.ignore(pos, expected))
        {
            is_object_id = true;
            continue;
        }

        break;
    }

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

    attribute_declaration->hierarchical = hierarchical;
    attribute_declaration->bidirectional = bidirectional;
    attribute_declaration->injective = injective;
    attribute_declaration->is_object_id = is_object_id;

    return true;
}


bool ParserDictionaryAttributeDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserDictionaryAttributeDeclaration>(),
        std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

}
