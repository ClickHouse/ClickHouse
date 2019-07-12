#pragma once

#include <Core/Types.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <ext/singleton.h>

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
class FormatFactory final : public ext::singleton<FormatFactory>
{
public:
    /// This callback allows to perform some additional actions after reading a single row.
    /// It's initial purpose was to extract payload for virtual columns from Kafka Consumer ReadBuffer.
    using ReadCallback = std::function<void()>;

private:
    using InputCreator = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        UInt64 rows_portion_size,
        ReadCallback callback,
        const FormatSettings & settings)>;

    using OutputCreator = std::function<BlockOutputStreamPtr(
        WriteBuffer & buf,
        const Block & sample,
        const Context & context,
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
            const FormatSettings & settings)>;

    using Creators = std::pair<InputCreator, OutputCreator>;
    using ProcessorCreators = std::pair<InputProcessorCreator, OutputProcessorCreator>;

    using FormatsDictionary = std::unordered_map<String, Creators>;
    using FormatProcessorsDictionary = std::unordered_map<String, ProcessorCreators>;

public:
    BlockInputStreamPtr getInput(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        UInt64 max_block_size,
        UInt64 rows_portion_size = 0,
        ReadCallback callback = {}) const;

    BlockOutputStreamPtr getOutput(const String & name, WriteBuffer & buf,
        const Block & sample, const Context & context) const;

    InputFormatPtr getInputFormat(const String & name, ReadBuffer & buf,
        const Block & sample, const Context & context, UInt64 max_block_size) const;

    OutputFormatPtr getOutputFormat(const String & name, WriteBuffer & buf,
        const Block & sample, const Context & context) const;

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
    FormatsDictionary dict;
    FormatProcessorsDictionary processors_dict;

    FormatFactory();
    friend class ext::singleton<FormatFactory>;

    const Creators & getCreators(const String & name) const;
    const ProcessorCreators & getProcessorCreators(const String & name) const;
};

}
