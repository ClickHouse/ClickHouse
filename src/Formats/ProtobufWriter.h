#pragma once

#include "config_formats.h"

#if USE_PROTOBUF
#   include <Core/Types.h>
#   include <Common/PODArray.h>


namespace DB
{
class WriteBuffer;

/// Utility class for writing in the Protobuf format.
/// Knows nothing about protobuf schemas, just provides useful functions to serialize data.
class ProtobufWriter
{
public:
    explicit ProtobufWriter(WriteBuffer & out_);
    ~ProtobufWriter();

    void startMessage();
    void endMessage(bool with_length_delimiter);

    void startNestedMessage();
    void endNestedMessage(int field_number, bool is_group, bool skip_if_empty);

    void writeInt(int field_number, Int64 value);
    void writeUInt(int field_number, UInt64 value);
    void writeSInt(int field_number, Int64 value);
    template <typename T>
    void writeFixed(int field_number, T value);
    void writeString(int field_number, std::string_view str);

    void startRepeatedPack();
    void endRepeatedPack(int field_number, bool skip_if_empty);

private:
    struct Piece
    {
        size_t start;
        size_t end;
        Piece(size_t start_, size_t end_) : start(start_), end(end_) {}
        Piece() = default;
    };

    struct NestedInfo
    {
        size_t num_pieces_at_start;
        size_t num_bytes_skipped_at_start;
        NestedInfo(size_t num_pieces_at_start_, size_t num_bytes_skipped_at_start_)
            : num_pieces_at_start(num_pieces_at_start_), num_bytes_skipped_at_start(num_bytes_skipped_at_start_)
        {
        }
    };

    WriteBuffer & out;
    PODArray<UInt8> buffer;
    std::vector<Piece> pieces;
    size_t current_piece_start = 0;
    size_t num_bytes_skipped = 0;
    std::vector<NestedInfo> nested_infos;
    bool in_repeated_pack = false;
};

}
#endif
