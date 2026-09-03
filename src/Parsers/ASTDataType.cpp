#include <Parsers/ASTDataType.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String ASTDataType::getID(char delim) const
{
    return "DataType" + (delim + name);
}

ASTPtr ASTDataType::clone() const
{
    auto res = make_intrusive<ASTDataType>(*this);
    cloneDataTypeChildrenTo(*res);
    return res;
}

ASTPtr ASTDataType::getArguments() const
{
    if (!children.empty() && children.front()->as<ASTExpressionList>())
        return children[0];
    return nullptr;
}

ASTPtr ASTDataType::getCodec() const
{
    const size_t argument_children = getArguments() ? 1 : 0;
    if (children.size() > argument_children)
        return children.back();
    return nullptr;
}

void ASTDataType::setCodec(ASTPtr codec)
{
    if (!codec)
    {
        resetCodecOperation();
        return;
    }

    /// The argument list, when present, is always the first child. Any following child is the codec.
    if (getCodec())
        children.back() = std::move(codec);
    else
        children.push_back(std::move(codec));
    flags<ASTDataTypeFlags>().remove_codec = false;
}

void ASTDataType::setCodecRemoval(bool value)
{
    if (getCodec())
        children.pop_back();
    flags<ASTDataTypeFlags>().remove_codec = value;
}

void ASTDataType::resetCodecOperation()
{
    setCodecRemoval(false);
}

void ASTDataType::cloneDataTypeChildrenTo(ASTDataType & target) const
{
    target.children.clear();
    if (const auto arguments = getArguments())
        target.children.push_back(arguments->clone());
    if (const auto codec = getCodec())
        target.children.push_back(codec->clone());
}

void ASTDataType::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DataType");
    w.writeString("name", name);
    if (auto args = getArguments())
        w.writeChild("arguments", args);
    writeCodecJSON(w);
}

void ASTDataType::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for ASTDataType");

    /// `arguments` is the `ASTExpressionList` produced by `ParserDataType`. `formatImpl` only prints
    /// the `(...)` when this child has its own `children`, so a non-list node here would be silently
    /// dropped (e.g. `Nullable(UInt8)` formatting as bare `Nullable`). Reject it at the JSON boundary.
    resetCodecOperation();
    children.clear();
    auto args = r.readChildOfType<ASTExpressionList>("arguments");
    if (args)
        children.push_back(args);
    readCodecJSON(r);
}

void ASTDataType::resetArguments()
{
    auto codec = getCodec();
    children.clear();
    if (codec)
        children.push_back(std::move(codec));
}

void ASTDataType::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    updateCodecHash(hash_state);
    /// Children are hashed automatically.
}

void ASTDataType::writeCodecJSON(JSONObjectWriter & writer) const
{
    if (const auto codec = getCodec())
        writer.writeChild("codec", codec);
    if (hasCodecRemoval())
        writer.writeBool("remove_codec", true);
}

void ASTDataType::readCodecJSON(JSONObjectReader & reader)
{
    auto codec = reader.readSpecialFunctionChild("codec", "CODEC");
    const bool remove_codec = reader.getBool("remove_codec");
    if (codec && remove_codec)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ASTDataType cannot set and remove CODEC at the same time");
    if (codec)
        setCodec(std::move(codec));
    else
        setCodecRemoval(remove_codec);
}

void ASTDataType::updateCodecHash(SipHash & hash_state) const
{
    hash_state.update(hasCodec());
    hash_state.update(hasCodecRemoval());
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

    formatCodecOperation(ostr, settings, state, frame);
}

void ASTDataType::formatCodecOperation(
    WriteBuffer & ostr,
    const FormatSettings & settings,
    FormatState & state,
    FormatStateStacked frame) const
{
    if (const auto codec = getCodec())
    {
        ostr << ' ';
        codec->format(ostr, settings, state, frame);
    }
    else if (hasCodecRemoval())
        ostr << " REMOVE CODEC";
}

}
