#include <Parsers/ASTCodec.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTCodec::ASTCodec(ASTs elements_)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children = std::move(elements_);
    children.push_back(std::move(list));
}

ASTExpressionList & ASTCodec::elements() const
{
    return children.at(0)->as<ASTExpressionList &>();
}

ASTPtr ASTCodec::clone() const
{
    auto res = make_intrusive<ASTCodec>(*this);
    res->cloneChildren();
    return res;
}

void ASTCodec::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Codec");
    w.writeChild("elements", children.at(0));
}

void ASTCodec::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    auto list = r.readNameWithParametersListChild("elements");
    if (!list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'elements' for Codec during AST JSON deserialization");
    children.push_back(std::move(list));
}

void ASTCodec::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.expression_list_prepend_whitespace = false;
    /// Parameters are formatted as calls (`tuple(1, 2)`, not `(1, 2)`), the form the stored metadata has always had.
    frame.allow_operators = false;
    ostr << "CODEC(";
    elements().format(ostr, settings, state, frame);
    ostr << ')';
}

}
