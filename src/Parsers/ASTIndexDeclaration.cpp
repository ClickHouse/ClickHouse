#include <Parsers/ASTIndexDeclaration.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ASTIndexDeclaration::ASTIndexDeclaration(ASTPtr expression, ASTPtr type, const String & name_)
    : name(name_)
{
    if (!expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration must have an expression");
    children.push_back(expression);

    if (type)
    {
        if (!dynamic_cast<const ASTFunction *>(type.get()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration type must be a function");
        children.push_back(type);
    }
}

ASTPtr ASTIndexDeclaration::clone() const
{
    ASTPtr expr = getExpression();
    if (expr)
        expr = expr->clone();

    ASTPtr type = getType();
    if (type)
        type = type->clone();

    auto res = std::make_shared<ASTIndexDeclaration>(expr, type, name);
    res->granularity = granularity;

    return res;
}

ASTPtr ASTIndexDeclaration::getExpression() const
{
    if (children.size() <= expression_idx)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration must have an expression");
    return children[expression_idx];
}

std::shared_ptr<ASTFunction> ASTIndexDeclaration::getType() const
{
    if (children.size() <= type_idx)
        return nullptr;
    auto func_ast = std::dynamic_pointer_cast<ASTFunction>(children[type_idx]);
    if (!func_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index declaration type must be a function");
    return func_ast;
}

void ASTIndexDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (auto expr = getExpression())
    {
        if (part_of_create_index_query)
        {
            if (expr->as<ASTExpressionList>())
            {
                ostr << "(";
                expr->formatImpl(ostr, s, state, frame);
                ostr << ")";
            }
            else
                expr->formatImpl(ostr, s, state, frame);
        }
        else
        {
            s.writeIdentifier(ostr, name, /*ambiguous=*/false);
            ostr << " ";
            expr->formatImpl(ostr, s, state, frame);
        }
    }

    if (auto type = getType())
    {
        ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
        type->formatImpl(ostr, s, state, frame);
    }

    if (granularity)
    {
        ostr << (s.hilite ? hilite_keyword : "") << " GRANULARITY " << (s.hilite ? hilite_none : "");
        ostr << granularity;
    }
}

}
