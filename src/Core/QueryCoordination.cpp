#include <Core/QueryCoordination.h>

#include <Common/Exception.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int UNKNOWN_PROTOCOL;
}

namespace
{

QueryCoordinationRequestKind readRequestKind(ReadBuffer & in)
{
    UInt64 value = 0;
    readVarUInt(value, in);
    if (value > static_cast<UInt64>(QueryCoordinationRequestKind::MAX))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown query coordination request kind: {}", value);
    return static_cast<QueryCoordinationRequestKind>(value);
}

QueryCoordinationRequestMode readRequestMode(ReadBuffer & in)
{
    UInt64 value = 0;
    readVarUInt(value, in);
    if (value > static_cast<UInt64>(QueryCoordinationRequestMode::MAX))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown query coordination request mode: {}", value);
    return static_cast<QueryCoordinationRequestMode>(value);
}

QueryCoordinationResponseMode readResponseMode(ReadBuffer & in)
{
    UInt64 value = 0;
    readVarUInt(value, in);
    if (value > static_cast<UInt64>(QueryCoordinationResponseMode::MAX))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown query coordination response mode: {}", value);
    return static_cast<QueryCoordinationResponseMode>(value);
}

}

void QueryCoordinationRequest::serialize(WriteBuffer & out, UInt64 peer_revision) const
{
    if (static_cast<UInt64>(kind) > static_cast<UInt64>(QueryCoordinationRequestKind::MAX))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown query coordination request kind: {}", static_cast<UInt64>(kind));
    if (version == 0 || version > CURRENT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Unsupported query coordination protocol version: {}", version);
    if (static_cast<UInt64>(mode) > static_cast<UInt64>(QueryCoordinationRequestMode::MAX))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown query coordination request mode: {}", static_cast<UInt64>(mode));
    if (mode == QueryCoordinationRequestMode::FallbackAll && payload.columns() != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "FallbackAll query coordination request contains a payload");

    writeVarUInt(request_id, out);
    writeVarUInt(static_cast<UInt64>(kind), out);
    writeVarUInt(version, out);
    writeVarUInt(static_cast<UInt64>(mode), out);

    if (mode == QueryCoordinationRequestMode::Candidates)
    {
        NativeWriter writer(out, peer_revision, std::make_shared<const Block>(payload.cloneEmpty()));
        writer.write(payload);
    }
}

QueryCoordinationRequest QueryCoordinationRequest::deserialize(ReadBuffer & in, UInt64 peer_revision)
{
    QueryCoordinationRequest request;
    readVarUInt(request.request_id, in);
    request.kind = readRequestKind(in);
    readVarUInt(request.version, in);
    if (request.version == 0 || request.version > CURRENT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Unsupported query coordination protocol version: {}", request.version);
    request.mode = readRequestMode(in);

    if (request.mode == QueryCoordinationRequestMode::FallbackAll)
        return request;
    if (in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Candidate query coordination request has no payload");

    NativeReader reader(in, peer_revision);
    request.payload = reader.read();
    return request;
}

void QueryCoordinationResponse::serialize(WriteBuffer & out) const
{
    if (static_cast<UInt64>(mode) > static_cast<UInt64>(QueryCoordinationResponseMode::MAX))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown query coordination response mode: {}", static_cast<UInt64>(mode));
    if (mode == QueryCoordinationResponseMode::FallbackAll && !selected_ordinals.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "FallbackAll query coordination response contains selected ordinals");

    writeVarUInt(request_id, out);
    writeVarUInt(static_cast<UInt64>(mode), out);
    writeVarUInt(selected_ordinals.size(), out);
    for (UInt64 ordinal : selected_ordinals)
        writeVarUInt(ordinal, out);
}

QueryCoordinationResponse QueryCoordinationResponse::deserialize(ReadBuffer & in, size_t candidate_rows)
{
    QueryCoordinationResponse response;
    readVarUInt(response.request_id, in);
    response.mode = readResponseMode(in);

    UInt64 size = 0;
    readVarUInt(size, in);
    if (response.mode == QueryCoordinationResponseMode::FallbackAll && size != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "FallbackAll query coordination response contains selected ordinals");
    if (size > candidate_rows)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Query coordination response contains {} selected ordinals for {} candidate rows",
            size,
            candidate_rows);

    response.selected_ordinals.reserve(static_cast<size_t>(size));
    for (UInt64 i = 0; i < size; ++i)
    {
        UInt64 ordinal = 0;
        readVarUInt(ordinal, in);
        if (ordinal >= candidate_rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Selected query coordination ordinal {} is outside the candidate range [0, {})",
                ordinal,
                candidate_rows);
        response.selected_ordinals.push_back(ordinal);
    }

    return response;
}

}
