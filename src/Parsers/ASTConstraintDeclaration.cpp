#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTConstraintDeclaration::clone() const
{
    auto res = make_intrusive<ASTConstraintDeclaration>();

    res->name = name;
    res->type = type;

    if (expr)
        res->set(res->expr, expr->clone());

    if (unique_columns)
        res->unique_columns = unique_columns->clone();

    res->unique_engine_type = unique_engine_type;

    return res;
}

void ASTConstraintDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ostr << backQuoteIfNeed(name);

    if (type == Type::UNIQUE)
    {
        ostr << " UNIQUE ";
        if (unique_columns)
        {
            ostr << "(";
            unique_columns->format(ostr, s, state, frame);
            ostr << ")";
        }
        if (!unique_engine_type.empty())
            ostr << " TYPE " << unique_engine_type;
    }
    else
    {
        ostr << (type == Type::CHECK ? " CHECK " : " ASSUME ");
        chassert(expr);
        auto nested_frame = frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(expr); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        expr->format(ostr, s, state, nested_frame);
    }
}

}

