#include <Parsers/ASTDefaultedColumn.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserInsertDefaultValue.h>

namespace DB
{


bool ParserDefaultValue::parseImpl(DB::IParser::Pos & pos, DB::ASTPtr & node, DB::Expected & expected)
{
    ParserExpression s_exp;
    ParserKeyword s_as(Keyword::AS);
    ParserColumnIdentifier s_col;
    auto DefaultElement = std::make_shared<ASTDefaultedColumn>();
    if (ASTPtr expression, name; s_exp.parse(pos, expression, expected) && s_as.ignore(pos, expected) && s_col.parse(pos, name, expected))
    {
        DefaultElement->expression = expression;
        DefaultElement->name = name;
        node = DefaultElement;
        return true;
    }
    return false;
}

bool ParserColumnIdentifier::parseImpl(DB::IParser::Pos & pos, DB::ASTPtr & node, DB::Expected & expected)
{
    return ParserColumnsMatcher().parse(pos, node, expected) || ParserCompoundIdentifier().parse(pos, node, expected);
}
}
