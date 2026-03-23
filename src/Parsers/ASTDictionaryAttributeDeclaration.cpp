#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{
ASTPtr ASTDictionaryAttributeDeclaration::clone() const
{
    const auto res = make_intrusive<ASTDictionaryAttributeDeclaration>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    if (default_value)
    {
        res->default_value = default_value;
        res->children.push_back(res->default_value);
    }

    if (expression)
    {
        res->expression = expression->clone();
        res->children.push_back(res->expression);
    }

    return res;
}

void ASTDictionaryAttributeDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DictionaryAttributeDeclaration");
    w.writeString("name", name);
    w.writeChild("attr_type", type);
    w.writeChild("default_value", default_value);
    w.writeChild("expression", expression);
    w.writeBool("hierarchical", hierarchical);
    w.writeBool("bidirectional", bidirectional);
    w.writeBool("injective", injective);
    w.writeBool("is_object_id", is_object_id);
}

void ASTDictionaryAttributeDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");

    type = r.readChild("attr_type");
    if (type)
        children.push_back(type);

    default_value = r.readChild("default_value");
    if (default_value)
        children.push_back(default_value);

    expression = r.readChild("expression");
    if (expression)
        children.push_back(expression);

    hierarchical = r.getBool("hierarchical");
    bidirectional = r.getBool("bidirectional");
    injective = r.getBool("injective");
    is_object_id = r.getBool("is_object_id");
}

void ASTDictionaryAttributeDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.writeIdentifier(ostr, name, /*ambiguous=*/true);

    if (type)
    {
        ostr << ' ';
        type->format(ostr, settings, state, frame);
    }

    if (default_value)
    {
        ostr << ' ' << "DEFAULT" << ' ';
        default_value->format(ostr, settings, state, frame);
    }

    if (expression)
    {
        ostr << ' ' << "EXPRESSION" << ' ';
        auto nested_frame = frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(expression.get()); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        expression->format(ostr, settings, state, nested_frame);
    }

    if (hierarchical)
        ostr << ' ' << "HIERARCHICAL";

    if (bidirectional)
        ostr << ' ' << "BIDIRECTIONAL";

    if (injective)
        ostr << ' ' << "INJECTIVE";

    if (is_object_id)
        ostr << ' ' << "IS_OBJECT_ID";
}

}
