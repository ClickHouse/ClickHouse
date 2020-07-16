#pragma once

#include <Core/Types.h>
#include <Columns/IColumn.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <IO/BufferWithOwnMemory.h>

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

    /** Fast reading data from buffer and save result to memory.
      * Reads at least min_chunk_bytes and some more until the end of the chunk, depends on the format.
      * Used in ParallelParsingBlockInputStream.
      */
    using FileSegmentationEngine = std::function<bool(
        ReadBuffer & buf,
        DB::Memory<> & memory,
        size_t min_chunk_bytes)>;

    /// This callback allows to perform some additional actions after writing a single row.
    /// It's initial purpose was to flush Kafka message for each row.
    using WriteCallback = std::function<void(
        const Columns & columns,
        size_t row)>;

private:
    using InputCreator = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        UInt64 max_block_size,
        ReadCallback callback,
        const FormatSettings & settings)>;

    using OutputCreator = std::function<BlockOutputStreamPtr(
        WriteBuffer & buf,
        const Block & sample,
        WriteCallback callback,
        const FormatSettings & settings)>;

    using InputProcessorCreator = std::function<InputFormatPtr(
            ReadBuffer & buf,
            const Block & header,
            const RowInputFormatParams & params,
            const FormatSettings & settings)>;

    using OutputProcessorCreator = std::function<OutputFormatPtr(
            WriteBuffer & buf,
            const Block & sample,
            WriteCallback callback,
            const FormatSettings & settings)>;

    struct Creators
    {
        InputCreator input_creator;
        OutputCreator output_creator;
        InputProcessorCreator input_processor_creator;
        OutputProcessorCreator output_processor_creator;
        FileSegmentationEngine file_segmentation_engine;
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
    void registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine);

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

/// Formats for both input/output.

void registerInputFormatNative(FormatFactory & factory);
void registerOutputFormatNative(FormatFactory & factory);

void registerInputFormatProcessorNative(FormatFactory & factory);
void registerOutputFormatProcessorNative(FormatFactory & factory);
void registerInputFormatProcessorRowBinary(FormatFactory & factory);
void registerOutputFormatProcessorRowBinary(FormatFactory & factory);
void registerInputFormatProcessorTabSeparated(FormatFactory & factory);
void registerOutputFormatProcessorTabSeparated(FormatFactory & factory);
void registerInputFormatProcessorValues(FormatFactory & factory);
void registerOutputFormatProcessorValues(FormatFactory & factory);
void registerInputFormatProcessorCSV(FormatFactory & factory);
void registerOutputFormatProcessorCSV(FormatFactory & factory);
void registerInputFormatProcessorTSKV(FormatFactory & factory);
void registerOutputFormatProcessorTSKV(FormatFactory & factory);
void registerInputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerInputFormatProcessorJSONCompactEachRow(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompactEachRow(FormatFactory & factory);
void registerInputFormatProcessorParquet(FormatFactory & factory);
void registerOutputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorArrow(FormatFactory & factory);
void registerOutputFormatProcessorArrow(FormatFactory & factory);
void registerInputFormatProcessorProtobuf(FormatFactory & factory);
void registerOutputFormatProcessorProtobuf(FormatFactory & factory);
void registerInputFormatProcessorAvro(FormatFactory & factory);
void registerOutputFormatProcessorAvro(FormatFactory & factory);
void registerInputFormatProcessorTemplate(FormatFactory & factory);
void registerOutputFormatProcessorTemplate(FormatFactory & factory);
void registerInputFormatProcessorMsgPack(FormatFactory & factory);
void registerOutputFormatProcessorMsgPack(FormatFactory & factory);
void registerInputFormatProcessorORC(FormatFactory & factory);
void registerOutputFormatProcessorORC(FormatFactory & factory);


/// File Segmentation Engines for parallel reading

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory);
void registerFileSegmentationEngineCSV(FormatFactory & factory);
void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory);
void registerFileSegmentationEngineRegexp(FormatFactory & factory);
void registerFileSegmentationEngineJSONAsString(FormatFactory & factory);

/// Output only (presentational) formats.

void registerOutputFormatNull(FormatFactory & factory);

void registerOutputFormatProcessorPretty(FormatFactory & factory);
void registerOutputFormatProcessorPrettyCompact(FormatFactory & factory);
void registerOutputFormatProcessorPrettySpace(FormatFactory & factory);
void registerOutputFormatProcessorPrettyASCII(FormatFactory & factory);
void registerOutputFormatProcessorVertical(FormatFactory & factory);
void registerOutputFormatProcessorJSON(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompact(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatProcessorXML(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver2(FormatFactory & factory);
void registerOutputFormatProcessorNull(FormatFactory & factory);
void registerOutputFormatProcessorMySQLWire(FormatFactory & factory);
void registerOutputFormatProcessorMarkdown(FormatFactory & factory);
void registerOutputFormatProcessorPostgreSQLWire(FormatFactory & factory);

/// Input only formats.
void registerInputFormatProcessorCapnProto(FormatFactory & factory);
void registerInputFormatProcessorRegexp(FormatFactory & factory);
void registerInputFormatProcessorJSONAsString(FormatFactory & factory);

}
