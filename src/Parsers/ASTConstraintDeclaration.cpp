#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

    if (!r.has("constraint_type"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'constraint_type' field in `ConstraintDeclaration` during AST JSON deserialization");

    const String constraint_type_str = r.getString("constraint_type");
    if (constraint_type_str == "CHECK")
        type = Type::CHECK;
    else if (constraint_type_str == "ASSUME")
        type = Type::ASSUME;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unknown 'constraint_type' value '{}' in `ConstraintDeclaration` during AST JSON deserialization", constraint_type_str);

    auto child = r.readChild("expr");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'expr' field in `ConstraintDeclaration` during AST JSON deserialization");
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
