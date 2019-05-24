#pragma once

#include <Core/Types.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <ext/singleton.h>
#include <IO/BufferWithOwnMemory.h>

#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{

class Block;
class Context;
struct FormatSettings;

class ReadBuffer;
class WriteBuffer;

/** Allows to create an IBlockInputStream or IBlockOutputStream by the name of the format.
  * Note: format and compression are independent things.
  */
class FormatFactory final : public ext::singleton<FormatFactory>
{
public:
    using FileSegmentationEngine = std::function<bool(
        ReadBuffer & buf,
        DB::Memory<> & memory,
        size_t min_size)>;

private:
    using InputCreator = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        UInt64 rows_portion_size,
        const FormatSettings & settings)>;

    using OutputCreator = std::function<BlockOutputStreamPtr(
        WriteBuffer & buf,
        const Block & sample,
        const Context & context,
        const FormatSettings & settings)>;

    struct Creators
    {
        InputCreator input_creator;
        OutputCreator output_creator;
        FileSegmentationEngine file_segmentation_engine;
    };

    using FormatsDictionary = std::unordered_map<String, Creators>;

public:
    BlockInputStreamPtr getInput(const String & name, ReadBuffer & buf,
        const Block & sample, const Context & context, UInt64 max_block_size, UInt64 rows_portion_size = 0) const;

    BlockOutputStreamPtr getOutput(const String & name, WriteBuffer & buf,
        const Block & sample, const Context & context) const;

    /// Register format by its name.
    void registerInputFormat(const String & name, InputCreator input_creator);
    void registerOutputFormat(const String & name, OutputCreator output_creator);
    void registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine);

    const FormatsDictionary & getAllFormats() const
    {
        return dict;
    }

private:
    FormatsDictionary dict;

    FormatFactory();
    friend class ext::singleton<FormatFactory>;

    const Creators & getCreators(const String & name) const;
};

}
