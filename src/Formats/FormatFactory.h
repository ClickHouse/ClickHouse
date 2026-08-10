#pragma once

#include <Formats/FormatSettings.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <base/types.h>
#include <Common/Allocator_fwd.h>
#include <Common/Documentation.h>
#include <Common/NamePrompter.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{

class Block;
struct Settings;
class SettingsChanges;
struct FormatFactorySettings;
struct ReadSettings;

class ReadBuffer;
class WriteBuffer;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class IInputFormat;
class IOutputFormat;
class IRowOutputFormat;

struct RowInputFormatParams;

class ISchemaReader;
class IExternalSchemaReader;
using SchemaReaderPtr = std::shared_ptr<ISchemaReader>;
using ExternalSchemaReaderPtr = std::shared_ptr<IExternalSchemaReader>;

using InputFormatPtr = std::shared_ptr<IInputFormat>;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;
using RowOutputFormatPtr = std::shared_ptr<IRowOutputFormat>;

template <typename Allocator>
struct Memory;

struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;

struct FormatFilterInfo;
using FormatFilterInfoPtr = std::shared_ptr<FormatFilterInfo>;

struct FileBucketInfo;
using FileBucketInfoPtr = std::shared_ptr<FileBucketInfo>;

struct IBucketSplitter;
using BucketSplitter = std::shared_ptr<IBucketSplitter>;

FormatSettings getFormatSettings(const ContextPtr & context);
FormatSettings getFormatSettings(const ContextPtr & context, const Settings & settings);

/** Resets the Parquet `field_id` settings (`output_format_parquet_column_field_ids` and
  * `output_format_parquet_auto_assign_field_ids`) to their defaults.
  *
  * A datalake table that carries its own column-id mapping (Iceberg) is the authoritative source of
  * `field_id`s, so ambient (server/profile/session) values of these settings are ignored for such
  * tables. They are reset before the `FormatSettings` are built, so that an ambient value never
  * reaches the `FormatSettings` of an Iceberg table at all.
  *
  * The two flags allow resetting each setting independently: a table definition may name one of the
  * settings explicitly, in which case only the other one carries an ambient value to ignore.
  */
void resetParquetFieldIdSettings(Settings & settings, bool reset_column_field_ids = true, bool reset_auto_assign_field_ids = true);

/// Same as `getFormatSettings(context)`, but with the Parquet `field_id` settings reset first.
FormatSettings getFormatSettingsIgnoringParquetFieldIds(const ContextPtr & context);

/** Builds the `FormatSettings` that a file-writing table engine (`File`, `URL`, `S3` and the other
  * object-storage engines) freezes at definition time: the settings of `context` plus the changes
  * from the `SETTINGS` clause of the definition, passed as `definition_changes` (`nullptr` when the
  * definition has no `SETTINGS` clause).
  *
  * The ambient (server, profile or session) values of the Parquet `field_id` settings are dropped:
  * only a value written in the definition itself expresses an intent to give this table its own
  * `field_id`s. An ambient value frozen onto a table it was never meant for makes the table
  * unwritable — for Iceberg because the table metadata provides the mapping instead, and for a plain
  * table because such a map was never validated against the table's columns, so every `INSERT` fails
  * in `ParquetBlockOutputFormat`. Each of the two settings is tracked separately: naming one of them
  * in the definition must not keep the other one's ambient value.
  */
FormatSettings getFormatSettingsForTableDefinition(const ContextPtr & context, const SettingsChanges * definition_changes);

/** Allows to create an IInputFormat or IOutputFormat by the name of the format.
  * Note: format and compression are independent things.
  */
class FormatFactory final : private boost::noncopyable, public IHints<2>
{
public:
    /** Fast reading data from buffer and save result to memory.
      * Reads at least `min_bytes` and some more until the end of the chunk, depends on the format.
      * If `max_rows` is non-zero the function also stops after reading the `max_rows` number of rows
      * (even if the `min_bytes` boundary isn't reached yet).
      * Used in ParallelParsingInputFormat.
      */
    using FileSegmentationEngine = std::function<std::pair<bool, size_t>(
        ReadBuffer & buf,
        DB::Memory<Allocator<false>> & memory,
        size_t min_bytes,
        size_t max_rows)>;

    using FileSegmentationEngineCreator = std::function<FileSegmentationEngine(
        const FormatSettings & settings)>;

    VectorWithMemoryTracking<String> getAllRegisteredNames() const override;
private:
    // On the input side, there are two kinds of formats:
    //  * InputCreator - formats parsed sequentially, e.g. CSV. Almost all formats are like this.
    //    FormatFactory uses ParallelReadBuffer to read in parallel, and ParallelParsingInputFormat
    //    to parse in parallel; the formats mostly don't need to worry about it.
    //  * RandomAccessInputCreator - column-oriented formats that require seeking back and forth in
    //    the file when reading. E.g. Parquet has metadata at the end of the file (needs to be read
    //    before we can parse any data), can skip columns by seeking in the file, and often reads
    //    many short byte ranges from the file. ParallelReadBuffer and ParallelParsingInputFormat
    //    are a poor fit. Instead, the format implementation is in charge of parallelizing both
    //    reading and parsing.

    using InputCreator = std::function<InputFormatPtr(
            ReadBuffer & buf,
            const Block & header,
            const RowInputFormatParams & params,
            const FormatSettings & settings)>;

    using FileBucketInfoCreator = std::function<FileBucketInfoPtr()>;

    using BucketSplitterCreator = std::function<BucketSplitter()>;

    // Incompatible with FileSegmentationEngine.
    using RandomAccessInputCreator = std::function<InputFormatPtr(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & settings,
        const ReadSettings & read_settings,
        bool is_remote_fs,
        FormatParserSharedResourcesPtr parser_shared_resources,
        FormatFilterInfoPtr format_filter_info)>;

    using RandomAccessInputCreatorWithMetadata = std::function<InputFormatPtr(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & settings,
        const ReadSettings & read_settings,
        bool is_remote_fs,
        FormatParserSharedResourcesPtr parser_shared_resources,
        FormatFilterInfoPtr format_filter_info,
        const std::optional<RelativePathWithMetadata> & object_with_metadata,
        const ContextPtr & context)>;

    using OutputCreator = std::function<OutputFormatPtr(
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        FormatFilterInfoPtr format_filter_info)>;

    /// Some input formats can have non trivial readPrefix() and readSuffix(),
    /// so in some cases there is no possibility to use parallel parsing.
    /// The checker should return true if parallel parsing should be disabled.
    using NonTrivialPrefixAndSuffixChecker = std::function<bool(ReadBuffer & buf)>;

    /// Some formats can support append depending on settings.
    /// The checker should return true if format support append.
    using AppendSupportChecker = std::function<bool(const FormatSettings & settings)>;

    /// Obtain HTTP content-type for the output format.
    using ContentTypeGetter = std::function<String(const std::optional<FormatSettings> & settings)>;

    using SchemaReaderCreator = std::function<SchemaReaderPtr(
        ReadBuffer & in,
        const FormatSettings & settings)>;

    using ExternalSchemaReaderCreator = std::function<ExternalSchemaReaderPtr(const FormatSettings & settings)>;

    /// Some formats can extract different schemas from the same source depending on
    /// some settings. To process this case in schema cache we should add some additional
    /// information to a cache key. This getter should return some string with information
    /// about such settings. For example, for Protobuf format it's the path to the schema
    /// and the name of the message.
    using AdditionalInfoForSchemaCacheGetter = std::function<String(const FormatSettings & settings)>;

    /// Some formats can support reading subset of columns depending on settings.
    /// The checker should return true if format support append.
    using SubsetOfColumnsSupportChecker = std::function<bool(const FormatSettings & settings)>;

    using PrewhereSupportChecker = std::function<bool(const FormatSettings & settings)>;

    struct Creators
    {
        String name;
        InputCreator input_creator;
        FileBucketInfoCreator file_bucket_info_creator;
        BucketSplitterCreator bucket_splitter_creator;
        RandomAccessInputCreator random_access_input_creator;
        RandomAccessInputCreatorWithMetadata random_access_input_creator_with_metadata;
        OutputCreator output_creator;
        FileSegmentationEngineCreator file_segmentation_engine_creator;
        SchemaReaderCreator schema_reader_creator;
        ExternalSchemaReaderCreator external_schema_reader_creator;
        bool supports_parallel_formatting{false};
        bool prefers_large_blocks{false};
        bool is_tty_friendly{true}; /// If false, client will ask before output in the terminal.
        ContentTypeGetter content_type = [](const std::optional<FormatSettings> &){ return "text/plain; charset=UTF-8"; };
        NonTrivialPrefixAndSuffixChecker non_trivial_prefix_and_suffix_checker;
        AppendSupportChecker append_support_checker;
        AdditionalInfoForSchemaCacheGetter additional_info_for_schema_cache_getter;
        SubsetOfColumnsSupportChecker subset_of_columns_support_checker;
        PrewhereSupportChecker prewhere_support_checker;
        Documentation documentation;
    };

    using FormatsDictionary = std::unordered_map<String, Creators>;
    using FileExtensionFormats = std::unordered_map<String, String>;

    InputFormatPtr getInputImpl(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        const ContextPtr & context,
        UInt64 max_block_size,
        const std::optional<RelativePathWithMetadata> & object_with_metadata,
        const std::optional<FormatSettings> & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources,
        FormatFilterInfoPtr format_filter_info,
        bool is_remote_fs,
        CompressionMethod compression,
        bool need_only_count,
        const std::optional<UInt64> & max_block_size_bytes,
        const std::optional<UInt64> & min_block_size_rows,
        const std::optional<UInt64> & min_block_size_bytes) const;
public:
    static FormatFactory & instance();

    /// This has two tricks up its sleeve:
    ///  * Parallel reading.
    ///    To enable it, make sure `buf` is a SeekableReadBuffer implementing readBigAt().
    ///  * Parallel parsing.
    /// `buf` must outlive the returned IInputFormat.
    /// The caller should make sure getFormatParsingThreadPool() is initialized.
    InputFormatPtr getInput(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        const ContextPtr & context,
        UInt64 max_block_size,
        const std::optional<FormatSettings> & format_settings = std::nullopt,
        FormatParserSharedResourcesPtr parser_shared_resources = nullptr,
        FormatFilterInfoPtr format_filter_info = nullptr,
        // affects things like buffer sizes and parallel reading
        bool is_remote_fs = false,
        // allows to do: buf -> parallel read -> decompression,
        // because parallel read after decompression is not possible
        CompressionMethod compression = CompressionMethod::None,
        bool need_only_count = false,
        const std::optional<UInt64> & max_block_size_bytes = std::nullopt,
        const std::optional<UInt64> & min_block_size_rows = std::nullopt,
        const std::optional<UInt64> & min_block_size_bytes = std::nullopt) const;

    /// much the same as getInput but allows for passing metadata from object storage
    InputFormatPtr getInputWithMetadata(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        const ContextPtr & context,
        UInt64 max_block_size,
        const std::optional<RelativePathWithMetadata> & object_with_metadata,
        const std::optional<FormatSettings> & format_settings = std::nullopt,
        FormatParserSharedResourcesPtr parser_shared_resources = nullptr,
        FormatFilterInfoPtr format_filter_info = nullptr,
        // affects things like buffer sizes and parallel reading
        bool is_remote_fs = false,
        // allows to do: buf -> parallel read -> decompression,
        // because parallel read after decompression is not possible
        CompressionMethod compression = CompressionMethod::None,
        bool need_only_count = false,
        const std::optional<UInt64> & max_block_size_bytes = std::nullopt,
        const std::optional<UInt64> & min_block_size_rows = std::nullopt,
        const std::optional<UInt64> & min_block_size_bytes = std::nullopt) const;

    /// Checks all preconditions. Returns ordinary format if parallel formatting cannot be done.
    OutputFormatPtr getOutputFormatParallelIfPossible(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        const ContextPtr & context,
        const std::optional<FormatSettings> & format_settings = std::nullopt,
        FormatFilterInfoPtr format_filter_info = nullptr) const;

    OutputFormatPtr getOutputFormat(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        const ContextPtr & context,
        const std::optional<FormatSettings> & _format_settings = std::nullopt,
        FormatFilterInfoPtr format_filter_info = nullptr) const;

    /// Creates a standalone JSONEachRow output format for debugging or testing.
    OutputFormatPtr getDefaultJSONEachRowOutputFormat(WriteBuffer & buf, const Block & sample) const;

    /// Content-Type to set when sending HTTP response with this output format.
    String getContentType(const String & name, const std::optional<FormatSettings> & settings) const;

    /// overload for formats that support object storage metadata
    SchemaReaderPtr getSchemaReader(
        const String & name,
        ReadBuffer & buf,
        const ContextPtr & context,
        const RelativePathWithMetadata & metadata,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    SchemaReaderPtr getSchemaReader(
        const String & name,
        ReadBuffer & buf,
        const ContextPtr & context,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    ExternalSchemaReaderPtr getExternalSchemaReader(
        const String & name,
        const ContextPtr & context,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    void registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine);

    void registerFileSegmentationEngineCreator(const String & name, FileSegmentationEngineCreator file_segmentation_engine_creator);

    void registerNonTrivialPrefixAndSuffixChecker(const String & name, NonTrivialPrefixAndSuffixChecker non_trivial_prefix_and_suffix_checker);

    void registerAppendSupportChecker(const String & name, AppendSupportChecker append_support_checker);

    /// If format always doesn't support append, you can use this method instead of
    /// registerAppendSupportChecker with append_support_checker that always returns true.
    void markFormatHasNoAppendSupport(const String & name);

    bool checkIfFormatSupportAppend(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_ = std::nullopt);

    /// Register format by its name.
    void registerInputFormat(const String & name, InputCreator input_creator);
    void registerRandomAccessInputFormat(const String & name, RandomAccessInputCreator input_creator);
    void registerRandomAccessInputFormatWithMetadata(const String & name, RandomAccessInputCreatorWithMetadata input_creator_with_metadata);
    void registerOutputFormat(const String & name, OutputCreator output_creator);

    /// Attach embedded documentation to a format by its name.
    void setDocumentation(const String & name, Documentation documentation);

    /// Register file extension for format
    void registerFileExtension(const String & extension, const String & format_name);
    String getFormatFromFileName(String file_name);
    std::optional<String> tryGetFormatFromFileName(String file_name);
    String getFormatFromFileDescriptor(int fd);
    std::optional<String> tryGetFormatFromFileDescriptor(int fd);

    /// Register schema readers for format its name.
    void registerSchemaReader(const String & name, SchemaReaderCreator schema_reader_creator);
    void registerExternalSchemaReader(const String & name, ExternalSchemaReaderCreator external_schema_reader_creator);

    void markOutputFormatSupportsParallelFormatting(const String & name);
    void markOutputFormatPrefersLargeBlocks(const String & name);
    void markOutputFormatNotTTYFriendly(const String & name);

    void setContentType(const String & name, const String & content_type);
    void setContentType(const String & name, ContentTypeGetter content_type);

    void markFormatSupportsSubsetOfColumns(const String & name);
    void registerSubsetOfColumnsSupportChecker(const String & name, SubsetOfColumnsSupportChecker subset_of_columns_support_checker);
    bool checkIfFormatSupportsSubsetOfColumns(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_ = std::nullopt) const;

    void registerPrewhereSupportChecker(const String & name, PrewhereSupportChecker prewhere_support_checker);
    bool checkIfFormatSupportsPrewhere(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_ = std::nullopt) const;

    bool checkIfFormatHasSchemaReader(const String & name) const;
    bool checkIfFormatHasExternalSchemaReader(const String & name) const;
    bool checkIfFormatHasAnySchemaReader(const String & name) const;
    bool checkIfOutputFormatPrefersLargeBlocks(const String & name) const;
    bool checkIfOutputFormatIsTTYFriendly(const String & name) const;

    bool checkParallelizeOutputAfterReading(const String & name, const ContextPtr & context) const;

    void registerAdditionalInfoForSchemaCacheGetter(const String & name, AdditionalInfoForSchemaCacheGetter additional_info_for_schema_cache_getter);
    String getAdditionalInfoForSchemaCache(const String & name, const ContextPtr & context, const std::optional<FormatSettings> & format_settings_ = std::nullopt);

    const FormatsDictionary & getAllFormats() const
    {
        return dict;
    }

    std::vector<String> getAllInputFormats() const;

    bool isInputFormat(const String & name) const;
    bool isOutputFormat(const String & name) const;

    /// Check that format with specified name exists and throw an exception otherwise.
    void checkFormatName(const String & name) const;
    bool exists(const String & name) const;

    FileBucketInfoPtr getFileBucketInfo(const String & format);
    void registerFileBucketInfo(const String & format, FileBucketInfoCreator bucket_info);
    void registerSplitter(const String & format, BucketSplitterCreator splitter);
    BucketSplitter getSplitter(const String & format);

private:
    FormatsDictionary dict;
    FileExtensionFormats file_extension_formats;

    const Creators & getCreators(const String & name) const;
    Creators & getOrCreateCreators(const String & name);

    // Creates a ReadBuffer to give to an input format. Returns nullptr if we should use `buf` directly.
    std::unique_ptr<ReadBuffer> wrapReadBufferIfNeeded(
        ReadBuffer & buf,
        CompressionMethod compression,
        const Creators & creators,
        const FormatSettings & format_settings,
        const Settings & settings,
        bool is_remote_fs,
        const FormatParserSharedResourcesPtr & parser_shared_resources) const;
};

}
