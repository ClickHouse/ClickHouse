#pragma once

#include <Common/Allocator.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <base/types.h>
#include <Core/NamesAndTypes.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <memory>
#include <unordered_map>

namespace DB
{

class Block;
struct Settings;
struct FormatFactorySettings;

class ReadBuffer;
class WriteBuffer;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class IInputFormat;
class IOutputFormat;

struct RowInputFormatParams;
struct RowOutputFormatParams;

class ISchemaReader;
class IExternalSchemaReader;
using SchemaReaderPtr = std::shared_ptr<ISchemaReader>;
using ExternalSchemaReaderPtr = std::shared_ptr<IExternalSchemaReader>;

using InputFormatPtr = std::shared_ptr<IInputFormat>;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

template <typename Allocator>
struct Memory;

FormatSettings getFormatSettings(ContextPtr context);

template <typename T>
FormatSettings getFormatSettings(ContextPtr context, const T & settings);

/** Allows to create an IInputFormat or IOutputFormat by the name of the format.
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
      * Used in ParallelParsingInputFormat.
      */
    using FileSegmentationEngine = std::function<std::pair<bool, size_t>(
        ReadBuffer & buf,
        DB::Memory<Allocator<false>> & memory,
        size_t min_chunk_bytes)>;

    /// This callback allows to perform some additional actions after writing a single row.
    /// It's initial purpose was to flush Kafka message for each row.
    using WriteCallback = std::function<void(
        const Columns & columns,
        size_t row)>;

private:
    using InputCreator = std::function<InputFormatPtr(
            ReadBuffer & buf,
            const Block & header,
            const RowInputFormatParams & params,
            const FormatSettings & settings)>;

    using OutputCreator = std::function<OutputFormatPtr(
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & settings)>;

    /// Some input formats can have non trivial readPrefix() and readSuffix(),
    /// so in some cases there is no possibility to use parallel parsing.
    /// The checker should return true if parallel parsing should be disabled.
    using NonTrivialPrefixAndSuffixChecker = std::function<bool(ReadBuffer & buf)>;

    /// Some formats can support append depending on settings.
    /// The checker should return true if format support append.
    using AppendSupportChecker = std::function<bool(const FormatSettings & settings)>;

    using SchemaReaderCreator = std::function<SchemaReaderPtr(ReadBuffer & in, const FormatSettings & settings)>;
    using ExternalSchemaReaderCreator = std::function<ExternalSchemaReaderPtr(const FormatSettings & settings)>;

    struct Creators
    {
        InputCreator input_creator;
        OutputCreator output_creator;
        FileSegmentationEngine file_segmentation_engine;
        SchemaReaderCreator schema_reader_creator;
        ExternalSchemaReaderCreator external_schema_reader_creator;
        bool supports_parallel_formatting{false};
        bool supports_subset_of_columns{false};
        NonTrivialPrefixAndSuffixChecker non_trivial_prefix_and_suffix_checker;
        AppendSupportChecker append_support_checker;
    };

    using FormatsDictionary = std::unordered_map<String, Creators>;
    using FileExtensionFormats = std::unordered_map<String, String>;

public:
    static FormatFactory & instance();

    InputFormatPtr getInput(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        ContextPtr context,
        UInt64 max_block_size,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    InputFormatPtr getInputFormat(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        ContextPtr context,
        UInt64 max_block_size,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    /// Checks all preconditions. Returns ordinary format if parallel formatting cannot be done.
    OutputFormatPtr getOutputFormatParallelIfPossible(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        ContextPtr context,
        WriteCallback callback = {},
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    OutputFormatPtr getOutputFormat(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        ContextPtr context,
        WriteCallback callback = {},
        const std::optional<FormatSettings> & _format_settings = std::nullopt) const;

    String getContentType(
        const String & name,
        ContextPtr context,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    SchemaReaderPtr getSchemaReader(
        const String & name,
        ReadBuffer & buf,
        ContextPtr & context,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    ExternalSchemaReaderPtr getExternalSchemaReader(
        const String & name,
        ContextPtr & context,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    void registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine);

    void registerNonTrivialPrefixAndSuffixChecker(const String & name, NonTrivialPrefixAndSuffixChecker non_trivial_prefix_and_suffix_checker);

    void registerAppendSupportChecker(const String & name, AppendSupportChecker append_support_checker);

    /// If format always doesn't support append, you can use this method instead of
    /// registerAppendSupportChecker with append_support_checker that always returns true.
    void markFormatHasNoAppendSupport(const String & name);

    bool checkIfFormatSupportAppend(const String & name, ContextPtr context, const std::optional<FormatSettings> & format_settings_ = std::nullopt);

    /// Register format by its name.
    void registerInputFormat(const String & name, InputCreator input_creator);
    void registerOutputFormat(const String & name, OutputCreator output_creator);

    /// Register file extension for format
    void registerFileExtension(const String & extension, const String & format_name);
    String getFormatFromFileName(String file_name, bool throw_if_not_found = false);
    String getFormatFromFileDescriptor(int fd);

    /// Register schema readers for format its name.
    void registerSchemaReader(const String & name, SchemaReaderCreator schema_reader_creator);
    void registerExternalSchemaReader(const String & name, ExternalSchemaReaderCreator external_schema_reader_creator);

    void markOutputFormatSupportsParallelFormatting(const String & name);
    void markFormatSupportsSubsetOfColumns(const String & name);

    bool checkIfFormatSupportsSubsetOfColumns(const String & name) const;

    bool checkIfFormatHasSchemaReader(const String & name) const;
    bool checkIfFormatHasExternalSchemaReader(const String & name) const;
    bool checkIfFormatHasAnySchemaReader(const String & name) const;

    const FormatsDictionary & getAllFormats() const
    {
        return dict;
    }

    bool isInputFormat(const String & name) const;
    bool isOutputFormat(const String & name) const;

    /// Check that format with specified name exists and throw an exception otherwise.
    void checkFormatName(const String & name) const;

private:
    FormatsDictionary dict;
    FileExtensionFormats file_extension_formats;

    const Creators & getCreators(const String & name) const;

};

}
