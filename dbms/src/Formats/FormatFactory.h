#pragma once

#include <Core/Types.h>
#include <DataStreams/IBlockStream_fwd.h>

#include <functional>
#include <memory>
#include <unordered_map>
#include <boost/noncopyable.hpp>


namespace DB
{

class Block;
class Context;
struct FormatSettings;

class ReadBuffer;
class WriteBuffer;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class IInputFormat;
class IOutputFormat;

struct RowInputFormatParams;

using InputFormatPtr = std::shared_ptr<IInputFormat>;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;


/** Allows to create an IBlockInputStream or IBlockOutputStream by the name of the format.
  * Note: format and compression are independent things.
  */
class FormatFactory final : private boost::noncopyable
{
public:
    /// This callback allows to perform some additional actions after reading a single row.
    /// It's initial purpose was to extract payload for virtual columns from Kafka Consumer ReadBuffer.
    using ReadCallback = std::function<void()>;

    /// This callback allows to perform some additional actions after writing a single row.
    /// It's initial purpose was to flush Kafka message for each row.
    using WriteCallback = std::function<void()>;

private:
    using InputCreator = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        ReadCallback callback,
        const FormatSettings & settings)>;

    using OutputCreator = std::function<BlockOutputStreamPtr(
        WriteBuffer & buf,
        const Block & sample,
        const Context & context,
        WriteCallback callback,
        const FormatSettings & settings)>;

    using InputProcessorCreator = std::function<InputFormatPtr(
            ReadBuffer & buf,
            const Block & header,
            const Context & context,
            const RowInputFormatParams & params,
            const FormatSettings & settings)>;

    using OutputProcessorCreator = std::function<OutputFormatPtr(
            WriteBuffer & buf,
            const Block & sample,
            const Context & context,
            WriteCallback callback,
            const FormatSettings & settings)>;

    struct Creators
    {
        InputCreator inout_creator;
        OutputCreator output_creator;
        InputProcessorCreator input_processor_creator;
        OutputProcessorCreator output_processor_creator;
    };

    using FormatsDictionary = std::unordered_map<String, Creators>;

public:

    static FormatFactory & instance();

    BlockInputStreamPtr getInput(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        ReadCallback callback = {}) const;

    BlockOutputStreamPtr getOutput(const String & name, WriteBuffer & buf,
        const Block & sample, const Context & context, WriteCallback callback = {}) const;

    InputFormatPtr getInputFormat(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        ReadCallback callback = {}) const;

    OutputFormatPtr getOutputFormat(
        const String & name, WriteBuffer & buf, const Block & sample, const Context & context, WriteCallback callback = {}) const;

    /// Register format by its name.
    void registerInputFormat(const String & name, InputCreator input_creator);
    void registerOutputFormat(const String & name, OutputCreator output_creator);

    void registerInputFormatProcessor(const String & name, InputProcessorCreator input_creator);
    void registerOutputFormatProcessor(const String & name, OutputProcessorCreator output_creator);

    const FormatsDictionary & getAllFormats() const
    {
        return dict;
    }

private:
    /// FormatsDictionary dict;
    FormatsDictionary dict;

    FormatFactory();

    const Creators & getCreators(const String & name) const;
};

}
