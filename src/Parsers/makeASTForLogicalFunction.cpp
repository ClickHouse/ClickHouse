#include <Parsers/makeASTForLogicalFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{

ASTPtr makeASTForLogicalAnd(ASTs && arguments)
{
    bool partial_result = true;
    boost::range::remove_erase_if(arguments, [&](const ASTPtr & argument) -> bool
    {
        bool b;
        if (!tryGetLiteralBool(argument.get(), b))
            return false;
        partial_result &= b;
        return true;
    });

    if (!partial_result)
        return std::make_shared<ASTLiteral>(Field{UInt8(0)});
    if (arguments.empty())
        return std::make_shared<ASTLiteral>(Field{UInt8(1)});
    if (arguments.size() == 1)
        return arguments[0];

    auto function = std::make_shared<ASTFunction>();
    auto exp_list = std::make_shared<ASTExpressionList>();
    function->name = "and";
    function->arguments = exp_list;
    function->children.push_back(exp_list);
    exp_list->children = std::move(arguments);
    return function;
}


ASTPtr makeASTForLogicalOr(ASTs && arguments)
{
    bool partial_result = false;
    boost::range::remove_erase_if(arguments, [&](const ASTPtr & argument) -> bool
    {
        bool b;
        if (!tryGetLiteralBool(argument.get(), b))
            return false;
        partial_result |= b;
        return true;
    });

    if (partial_result)
        return std::make_shared<ASTLiteral>(Field{UInt8(1)});
    if (arguments.empty())
        return std::make_shared<ASTLiteral>(Field{UInt8(0)});
    if (arguments.size() == 1)
        return arguments[0];

    auto function = std::make_shared<ASTFunction>();
    auto exp_list = std::make_shared<ASTExpressionList>();
    function->name = "or";
    function->arguments = exp_list;
    function->children.push_back(exp_list);
    exp_list->children = std::move(arguments);
    return function;
}


bool tryGetLiteralBool(const IAST * ast, bool & value)
{
    if (!ast)
        return false;

    try
    {
        if (const ASTLiteral * literal = ast->as<ASTLiteral>())
        {
            value = !literal->value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), literal->value);
            return true;
        }
        return false;
    }
    catch (...)
    {
        return false;
    }
}

}
