#include <Parsers/ASTNamedArgument.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTNamedArgument::clone() const
{
    auto res = make_intrusive<ASTNamedArgument>(*this);
    res->children = { value()->clone() };
    return res;
}

void ASTNamedArgument::appendColumnName(WriteBuffer & ostr) const
{
    writeString(name, ostr);
    writeCString(" => ", ostr);
    value()->appendColumnName(ostr);
}

void ASTNamedArgument::appendColumnNameWithoutAlias(WriteBuffer & ostr) const
{
    writeString(name, ostr);
    writeCString(" => ", ostr);
    value()->appendColumnNameWithoutAlias(ostr);
}

void ASTNamedArgument::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    ostr << " => ";

    if (auto ast = boost::dynamic_pointer_cast<ASTWithAlias>(value()); ast && !ast->alias.empty())
        frame.need_parens = true;

    value()->format(ostr, settings, state, frame);
}

void ASTNamedArgument::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "NamedArgument");
    w.writeString("name", name);
    w.writeChildren(children);
}

void ASTNamedArgument::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    name = r.getString("name");
    children = r.readExpressionChildren();
    if (children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "NamedArgument JSON must contain exactly one expression child, got {}", children.size());
}

}
