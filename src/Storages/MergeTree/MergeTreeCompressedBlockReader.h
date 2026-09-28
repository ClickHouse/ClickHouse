#pragma once

#include <Compression/CompressedReadBufferBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <optional>

namespace DB
{

class MergeTreeCompressedBlockReader : private CompressedReadBufferBase
{
public:
    struct Block
    {
        const char * payload;

        size_t compressed_bytes;

        size_t decompressed_bytes;
    };

    /// Reads the stream of `column` at `substream_path`: the column's own file when it is empty.
    MergeTreeCompressedBlockReader(
        const IMergeTreeDataPart & part,
        const NameAndTypePair & column,
        const ReadSettings & read_settings,
        const ISerialization::SubstreamPath & substream_path = {});

    static std::unique_ptr<ReadBufferFromFileBase> openColumnFile(
        const IMergeTreeDataPart & part,
        const NameAndTypePair & column,
        const ReadSettings & read_settings,
        const ISerialization::SubstreamPath & substream_path = {});

    std::optional<Block> next();

    std::optional<UInt8> methodByte() const { return method_byte; }

private:
    std::unique_ptr<ReadBufferFromFileBase> file;
    std::optional<UInt8> method_byte;
};

}
