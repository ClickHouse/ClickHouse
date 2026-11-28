#include <Parsers/ASTCodecDeclaration.h>

#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

ASTCodecDeclaration::ASTCodecDeclaration(const String & name_, ASTPtr codec_)
    : name(name_)
{
    if (!codec_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Codec declaration must have a codec");
    children.push_back(std::move(codec_));
}

ASTPtr ASTCodecDeclaration::clone() const
{
    return make_intrusive<ASTCodecDeclaration>(name, getCodec()->clone());
}

void ASTCodecDeclaration::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `name` is not a child, so the default implementation does not see it.
    /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
    static_assert(sizeof(void *) != 8 || sizeof(*this) == 56, "If members were added to ASTCodecDeclaration, hash them here unless they are purely cosmetic.");
    hash_state.update(name.size());
    hash_state.update(name);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

ASTPtr ASTCodecDeclaration::getCodec() const
{
    if (children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Codec declaration must have a codec");
    return children[0];
}

void ASTCodecDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CodecDeclaration");
    w.writeString("name", name);
    w.writeChild("codec", getCodec());
}

void ASTCodecDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");

    auto codec = r.readSpecialFunctionChild("codec", "CODEC");
    if (!codec)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec declaration must have a codec during AST JSON deserialization");
    children.push_back(codec);
}

void ASTCodecDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.writeIdentifier(ostr, name, /*ambiguous=*/true);

    /// The keyword `CODEC` is printed by the column list, so only the chain in brackets is printed here.
    const auto & codec = getCodec()->as<const ASTFunction &>();
    frame.allow_operators = false;
    ostr << " (";
    codec.arguments->format(ostr, s, state, frame);
    ostr << ")";
}

}
