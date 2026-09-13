#include <Parsers/ASTTupleElementCodecOperation.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

const ASTPtr & ASTTupleElementCodecOperation::getCodec() const
{
    static const ASTPtr no_codec;
    return kind == TupleElementCodecOperationKind::Set && children.size() == 1 ? children.front() : no_codec;
}

void ASTTupleElementCodecOperation::validate() const
{
    if (kind == TupleElementCodecOperationKind::Set)
    {
        if (children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element CODEC set operation must contain exactly one codec expression");

        const auto * function = children.front() ? children.front()->as<ASTFunction>() : nullptr;
        if (!function || function->name != "CODEC")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element CODEC set operation must contain a CODEC expression");
    }
    else if (!children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element REMOVE CODEC operation cannot contain a codec expression");
}

String ASTTupleElementCodecOperation::getID(char delim) const
{
    return "TupleElementCodecOperation" + (delim + std::to_string(element_index));
}

ASTPtr ASTTupleElementCodecOperation::clone() const
{
    auto result = make_intrusive<ASTTupleElementCodecOperation>(*this);
    result->children.clear();
    for (const auto & child : children)
        result->children.push_back(child->clone());
    return result;
}

void ASTTupleElementCodecOperation::writeJSON(WriteBuffer & out) const
{
    validate();
    JSONObjectWriter writer(out, "TupleElementCodecOperation");
    writer.writeUInt("element_index", element_index);
    writer.writeString("kind", kind == TupleElementCodecOperationKind::Set ? "set" : "remove");
    writer.writeChild("codec", getCodec());
}

void ASTTupleElementCodecOperation::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader reader(json);
    if (!reader.has("element_index"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element CODEC operation requires an element index");
    const UInt64 index = reader.getUInt("element_index");
    const size_t converted_index = static_cast<size_t>(index);
    if (static_cast<UInt64>(converted_index) != index)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element CODEC operation index is out of range");
    element_index = converted_index;

    const String kind_name = reader.getString("kind");
    children.clear();
    if (kind_name == "set")
    {
        kind = TupleElementCodecOperationKind::Set;
        auto codec = reader.readSpecialFunctionChild("codec", "CODEC");
        if (!codec)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element CODEC set operation requires a codec expression");
        children.push_back(std::move(codec));
    }
    else if (kind_name == "remove")
    {
        kind = TupleElementCodecOperationKind::Remove;
        if (reader.has("codec"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple element REMOVE CODEC operation cannot contain a codec expression");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Tuple element CODEC operation kind '{}'", kind_name);

    validate();
}

void ASTTupleElementCodecOperation::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    hash_state.update(element_index);
    hash_state.update(static_cast<UInt8>(kind));
}

void ASTTupleElementCodecOperation::formatImpl(
    WriteBuffer & out,
    const FormatSettings & settings,
    FormatState & state,
    FormatStateStacked frame) const
{
    validate();
    if (kind == TupleElementCodecOperationKind::Remove)
        out << "REMOVE CODEC";
    else
        getCodec()->format(out, settings, state, frame);
}

}
