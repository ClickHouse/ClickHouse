#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


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

void ASTConstraintDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ConstraintDeclaration");
    w.writeString("name", name);
    w.writeString("constraint_type", std::string_view(type == Type::CHECK ? "CHECK" : "ASSUME"));
    w.writeChild("expr", expr);
}

void ASTConstraintDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");

    String constraint_type_str = r.getString("constraint_type");
    type = (constraint_type_str == "ASSUME") ? Type::ASSUME : Type::CHECK;

    auto child = r.readChild("expr");
    if (child)
        set(expr, child);
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
