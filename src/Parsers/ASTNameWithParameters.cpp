#include <Parsers/ASTNameWithParameters.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>
#include <Common/checkStackSize.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
}

ASTNameWithParameters::ASTNameWithParameters(String name_, ASTs parameters_)
    : name(std::move(name_))
{
    if (!parameters_.empty())
    {
        auto list = make_intrusive<ASTExpressionList>();
        list->children = std::move(parameters_);
        children.push_back(std::move(list));
    }
}

ASTPtr ASTNameWithParameters::clone() const
{
    auto res = make_intrusive<ASTNameWithParameters>(*this);
    res->cloneChildren();
    return res;
}

void ASTNameWithParameters::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "NameWithParameters");
    w.writeString("name", name);
    w.writeChild("parameters", getParameters());
}

void ASTNameWithParameters::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for NameWithParameters during AST JSON deserialization");

    /// `LZ4()` parses as `LZ4`, so an empty list is dropped to keep the tree hash of the parsed form.
    if (auto parameters = r.readChildOfType<ASTExpressionList>("parameters"); parameters && !parameters->children.empty())
        children.push_back(std::move(parameters));
}

void rejectLegacyNameSyntax(const IAST & ast)
{
    checkStackSize();

    if (const auto * name = ast.as<ASTNameWithParameters>(); name && name->legacy_syntax)
        throw Exception(ErrorCodes::SYNTAX_ERROR,
            "'{}' is a codec, statistics or index type name, not a function call: a second parameter list, "
            "`IGNORE NULLS` / `RESPECT NULLS` and `OVER` are not allowed", name->name);

    for (const auto & child : ast.children)
    {
        if (child)
            rejectLegacyNameSyntax(*child);
    }
}

void ASTNameWithParameters::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << name;
    if (auto parameters = getParameters())
    {
        frame.expression_list_prepend_whitespace = false;
        ostr << '(';
        parameters->format(ostr, settings, state, frame);
        ostr << ')';
    }
}

}
