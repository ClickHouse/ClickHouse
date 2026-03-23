#include <Parsers/ASTDataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

String ASTDataType::getID(char delim) const
{
    return "DataType" + (delim + name);
}

ASTPtr ASTDataType::clone() const
{
    auto res = make_intrusive<ASTDataType>(*this);
    const auto & arguments = getArguments();
    res->children.clear();

    if (arguments)
        res->children.push_back(arguments->clone());

    return res;
}

ASTPtr ASTDataType::getArguments() const
{
    if (!children.empty())
        return children[0];
    return nullptr;
}

void ASTDataType::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DataType");
    w.writeString("name", name);
    if (auto args = getArguments())
        w.writeChild("arguments", args);
}

void ASTDataType::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");

    auto args = r.readChild("arguments");
    if (args)
        children.push_back(args);
}

void ASTDataType::resetArguments()
{
    children.clear();
}

void ASTDataType::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    /// Children are hashed automatically.
}

void ASTDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << name;

    const auto & arguments = getArguments();
    if (arguments && !arguments->children.empty())
    {
        ostr << '(';

        if (!settings.one_line && settings.print_pretty_type_names && name == "Tuple")
        {
            ++frame.indent;
            std::string indent_str = settings.one_line ? "" : "\n" + std::string(4 * frame.indent, ' ');
            for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
            {
                if (i != 0)
                    ostr << ',';
                ostr << indent_str;
                arguments->children[i]->format(ostr, settings, state, frame);
            }
        }
        else
        {
            frame.expression_list_prepend_whitespace = false;
            arguments->format(ostr, settings, state, frame);
        }

        ostr << ')';
    }
}

}
