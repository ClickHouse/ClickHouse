#include <Parsers/MySQL/ASTDeclareReference.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

namespace MySQLParser
{

bool parseReferenceOption(IParser::Pos & pos, ASTDeclareReference::ReferenceOption & option, Expected & expected)
{
    if (ParserKeyword("RESTRICT").ignore(pos, expected))
        option = ASTDeclareReference::RESTRICT;
    else if (ParserKeyword("CASCADE").ignore(pos, expected))
        option = ASTDeclareReference::CASCADE;
    else if (ParserKeyword("SET NULL").ignore(pos, expected))
        option = ASTDeclareReference::SET_NULL;
    else if (ParserKeyword("NO ACTION").ignore(pos, expected))
        option = ASTDeclareReference::NO_ACTION;
    else if (ParserKeyword("SET DEFAULT").ignore(pos, expected))
        option = ASTDeclareReference::SET_DEFAULT;
    else
        return false;

    return true;
}

ASTPtr ASTDeclareReference::clone() const
{
    auto res = std::make_shared<ASTDeclareReference>(*this);
    res->children.clear();

    if (reference_expression)
    {
        res->reference_expression = reference_expression->clone();
        res->children.emplace_back(res->reference_expression);
    }

    return res;
}

bool ParserDeclareReference::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr table_name;
    ASTPtr expression;
    ParserExpression p_expression;
    ParserIdentifier p_identifier;
    ASTDeclareReference::MatchKind match_kind = ASTDeclareReference::MATCH_FULL;
    ASTDeclareReference::ReferenceOption delete_option = ASTDeclareReference::RESTRICT;
    ASTDeclareReference::ReferenceOption update_option = ASTDeclareReference::RESTRICT;

    if (!ParserKeyword("REFERENCES").ignore(pos, expected))
        return false;

    if (!p_identifier.parse(pos, table_name, expected))
        return false;

    if (!p_expression.parse(pos, expression, expected))
        return false;

    if (ParserKeyword("MATCH").ignore(pos, expected))
    {
        if (ParserKeyword("FULL").ignore(pos, expected))
            match_kind = ASTDeclareReference::MATCH_FULL;
        else if (ParserKeyword("SIMPLE").ignore(pos, expected))
            match_kind = ASTDeclareReference::MATCH_SIMPLE;
        else if (ParserKeyword("PARTIAL").ignore(pos, expected))
            match_kind = ASTDeclareReference::MATCH_PARTIAL;
        else
            return false;
    }

    while (true)
    {
        if (ParserKeyword("ON DELETE").ignore(pos, expected))
        {
            if (!parseReferenceOption(pos, delete_option, expected))
                return false;
        }
        else if (ParserKeyword("ON UPDATE").ignore(pos, expected))
        {
            if (!parseReferenceOption(pos, update_option, expected))
                return false;
        }
        else
            break;
    }

    auto declare_reference = std::make_shared<ASTDeclareReference>();
    declare_reference->kind = match_kind;
    declare_reference->on_delete_option = delete_option;
    declare_reference->on_update_option = update_option;
    declare_reference->reference_expression = expression;
    declare_reference->reference_table_name = table_name->as<ASTIdentifier>()->name();

    node = declare_reference;
    return true;
}
}

}
