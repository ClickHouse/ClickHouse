#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/SipHash.h>
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

    return res;
}

void ASTConstraintDeclaration::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `name` and `type` (CHECK/ASSUME) are not children, so the default implementation does not see them.
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(type);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTConstraintDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ostr << backQuoteIfNeed(name);
    ostr << (type == Type::CHECK ? " CHECK " : " ASSUME ");
    chassert(expr);
    auto nested_frame = frame;
    if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(expr); ast_alias && !ast_alias->tryGetAlias().empty())
        nested_frame.need_parens = true;
    expr->format(ostr, s, state, nested_frame);
}

}
