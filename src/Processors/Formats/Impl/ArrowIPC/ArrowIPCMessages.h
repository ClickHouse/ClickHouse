#pragma once

#include "config.h"

#if USE_ARROW

#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Common/VectorWithMemoryTracking.h>

#include <functional>
#include <optional>
#include <string_view>

namespace DB::ArrowIPC
{

/// Arrow IPC messages without their stream framing, as Arrow Flight carries them (`FlightData.data_header` and
/// `FlightData.data_body`): the FlatBuffer `Message` and the body.
struct IPCMessage
{
    String header;
    String body;
};

/// A message in the encapsulated IPC form: the continuation marker, the length, the FlatBuffer padded to 8 bytes,
/// and the body.
String frameMessage(const IPCMessage & message);

/// Splits an Arrow IPC stream (the output of the `ArrowStream` format) into its messages. The end-of-stream
/// marker, if any, ends the stream.
VectorWithMemoryTracking<IPCMessage> splitStream(std::string_view stream);

/// The `Schema` message of `schema` in the encapsulated IPC form: the continuation marker, the length, the
/// FlatBuffer and its padding to 8 bytes. This is how Arrow Flight serializes a schema (`FlightInfo.schema`,
/// `SchemaResult.schema`, and the schemas of Arrow Flight SQL).
String serializeSchemaMessage(const ArrowSchema & schema);

/// The same for the schema the native `Arrow` writer writes for these columns.
String serializeSchemaMessage(const Names & names, const DataTypes & types, const FormatSettings & settings);

/// An Arrow IPC stream written by the native `ArrowStream` writer.
struct EncodedStream
{
    IPCMessage schema;
    /// The dictionary and record batch messages, in order.
    VectorWithMemoryTracking<IPCMessage> batches;
    size_t num_rows = 0;
    /// The size of the bodies of the batches.
    size_t num_bytes = 0;
};

/// Encodes `chunks` of `header` with the native `ArrowStream` writer.
EncodedStream encodeStream(const Block & header, const Chunks & chunks, const FormatSettings & settings);

/// Parses a `Schema` message, encapsulated or a bare FlatBuffer `Message`.
ArrowSchema parseSchemaMessage(std::string_view message);

/// The number of rows that the header of a message (the FlatBuffer `Message`) declares for its record batch, or
/// nothing if it is not a message with a record batch. It is known before the batch is decoded.
std::optional<Int64> recordBatchRows(std::string_view header);

/// Reads the messages of an Arrow IPC stream, given one by one, as the stream itself: the `ArrowStream` input
/// format reads Arrow Flight data through it. `next` returns the next message, or false at the end.
class IPCMessagesReadBuffer : public ReadBuffer
{
public:
    using NextMessage = std::function<bool(IPCMessage & message)>;

    explicit IPCMessagesReadBuffer(NextMessage next_message_);

private:
    bool nextImpl() override;

    NextMessage next_message;
    /// The current message, framed.
    String current;
    bool finished = false;
};

}

#endif
