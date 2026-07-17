#include <Parsers/MySQL/ASTDeclareConstraint.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTDeclareConstraint::clone() const
{
    auto res = std::make_shared<ASTDeclareConstraint>(*this);
    res->children.clear();

    if (check_expression)
    {
        res->check_expression = check_expression->clone();
        res->children.emplace_back(res->check_expression);
    }

    return res;
}

bool ParserDeclareConstraint::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    bool enforced = true;
    ASTPtr constraint_symbol;
    ASTPtr index_check_expression;
    ParserExpression p_expression;

    if (ParserKeyword(Keyword::CONSTRAINT).ignore(pos, expected))
    {
        if (!ParserKeyword(Keyword::CHECK).checkWithoutMoving(pos, expected))
            ParserIdentifier().parse(pos, constraint_symbol, expected);
    }


    if (!ParserKeyword(Keyword::CHECK).ignore(pos, expected))
        return false;

    if (!p_expression.parse(pos, index_check_expression, expected))
        return false;

    if (ParserKeyword(Keyword::NOT).ignore(pos, expected))
    {
        if (!ParserKeyword(Keyword::ENFORCED).ignore(pos, expected))
            return false;

        enforced = false;
    }
    else
    {
        enforced = true;
        ParserKeyword(Keyword::ENFORCED).ignore(pos, expected);
    }

    auto declare_constraint = std::make_shared<ASTDeclareConstraint>();
    declare_constraint->enforced = enforced;
    declare_constraint->check_expression = index_check_expression;

    if (constraint_symbol)
        declare_constraint->constraint_name = constraint_symbol->as<ASTIdentifier>()->name();

    node = declare_constraint;
    return true;
}

}

}
