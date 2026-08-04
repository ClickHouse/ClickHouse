#include <Parsers/makeASTForLogicalFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/IAST_erase.h>
#include <Common/FieldVisitorConvertToNumber.h>


namespace DB
{

ASTPtr makeASTForLogicalAnd(ASTs && arguments)
{
    bool partial_result = true;
    std::erase_if(arguments, [&](const ASTPtr & argument)
    {
        bool b = false;
        if (!tryGetLiteralBool(argument.get(), b))
            return false;
        partial_result &= b;
        return true;
    });

    if (!partial_result)
        return make_intrusive<ASTLiteral>(Field{static_cast<UInt8>(0)});
    if (arguments.empty())
        return make_intrusive<ASTLiteral>(Field{static_cast<UInt8>(1)});
    if (arguments.size() == 1)
        return arguments[0];

    auto function = make_intrusive<ASTFunction>();
    auto exp_list = make_intrusive<ASTExpressionList>();
    function->name = "and";
    function->arguments = exp_list;
    function->children.push_back(exp_list);
    exp_list->children = std::move(arguments);
    return function;
}


ASTPtr makeASTForLogicalOr(ASTs && arguments)
{
    bool partial_result = false;
    std::erase_if(arguments, [&](const ASTPtr & argument)
    {
        bool b = false;
        if (!tryGetLiteralBool(argument.get(), b))
            return false;
        partial_result |= b;
        return true;
    });

    if (partial_result)
        return make_intrusive<ASTLiteral>(Field{static_cast<UInt8>(1)});
    if (arguments.empty())
        return make_intrusive<ASTLiteral>(Field{static_cast<UInt8>(0)});
    if (arguments.size() == 1)
        return arguments[0];

    auto function = make_intrusive<ASTFunction>();
    auto exp_list = make_intrusive<ASTExpressionList>();
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

    const ASTLiteral * literal = ast->as<ASTLiteral>();
    if (!literal)
        return false;

    /// `FieldVisitorConvertToNumber` throws for anything that is not a number, and a throw is the
    /// wrong way to ask whether this literal happens to be one - so ask the field directly.
    switch (literal->value.getType())
    {
        case Field::Types::Null:
            value = false;
            return true;
        case Field::Types::Bool:
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64:
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UInt256:
        case Field::Types::Int256:
            value = applyVisitor(FieldVisitorConvertToNumber<bool>(), literal->value);
            return true;
        default:
            return false;
    }
}

}
