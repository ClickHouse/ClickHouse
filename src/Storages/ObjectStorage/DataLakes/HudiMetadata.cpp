#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/find_symbols.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>
#include <Common/FailPoint.h>
#include <Common/Hex.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>

#if USE_AVRO
#include <GenericDatum.hh>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#endif

#if USE_PARQUET
#include <Core/Block.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#endif

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace FailPoints
{
    extern const char hudi_pause_before_iterate[];
    extern const char hudi_pause_in_listing_data_files[];
    extern const char hudi_pause_after_listing_timeline[];
}

namespace
{

/// Relative partition path ("" for a non-partitioned table) -> file ids.
using ReplacedFileGroups = std::unordered_map<String, std::unordered_set<String>>;

struct ActiveTimeline
{
    String directory;
    bool layout_2 = false;
};

bool isNumber(std::string_view s)
{
    return !s.empty() && std::all_of(s.begin(), s.end(), isNumericASCII);
}

/// The directory of `key` relative to the table root `root`, "" for a file directly in `root`.
std::string_view directoryInTable(std::string_view key, std::string_view root)
{
    if (key.starts_with(root))
        key.remove_prefix(root.size());
    size_t slash = key.rfind('/');
    return slash == std::string_view::npos ? "" : key.substr(0, slash);
}

struct CompletedInstant
{
    std::string_view requested;
    std::string_view completion;
    std::string_view action;
};

/// `<digits>.<action>`, and in layout 2 also `<requested time>_<completion time>.<action>`.
std::optional<CompletedInstant> parseCompletedInstantName(std::string_view file_name, bool with_completion_time)
{
    size_t dot = file_name.find('.');
    if (dot == std::string_view::npos)
        return std::nullopt;
    std::string_view time = file_name.substr(0, dot);
    std::string_view action = file_name.substr(dot + 1);
    if (action.empty() || action.contains('.'))
        return std::nullopt;

    size_t separator = time.find('_');
    if (separator == std::string_view::npos)
    {
        if (!isNumber(time))
            return std::nullopt;
        return CompletedInstant{time, time, action};
    }
    std::string_view requested = time.substr(0, separator);
    std::string_view completion = time.substr(separator + 1);
    if (!with_completion_time || !isNumber(requested) || !isNumber(completion))
        return std::nullopt;
    return CompletedInstant{requested, completion, action};
}

/// {file id, instant} of a log file `.<file id>_<instant>.log.<version>[_<write token>]`, CDC logs excluded.
std::optional<std::pair<std::string_view, std::string_view>> parseLogFileName(std::string_view file_name)
{
    static const re2::RE2 pattern(R"(\.([^._]+)_([^.]*)\.log\.\d+(?:_\d+-\d+-\d+)?)");
    std::string_view file_id;
    std::string_view instant;
    if (!re2::RE2::FullMatch(file_name, pattern, &file_id, &instant))
        return std::nullopt;
    return std::pair{file_id, instant};
}

#if USE_PARQUET
/// {minimum, maximum} requested time of the instants in a timeline history file `<min>_<max>_<level>.parquet`.
std::optional<std::pair<std::string_view, std::string_view>> parseHistoryFileName(std::string_view file_name)
{
    static const re2::RE2 pattern(R"((\d+)_(\d+)_\d+\.parquet)");
    std::string_view min_time;
    std::string_view max_time;
    if (!re2::RE2::FullMatch(file_name, pattern, &min_time, &max_time))
        return std::nullopt;
    return std::pair{min_time, max_time};
}
#endif

/// `<version>` of a timeline history manifest `manifest_<version>`.
std::optional<UInt64> parseManifestVersion(std::string_view file_name)
{
    constexpr std::string_view prefix = "manifest_";
    UInt64 version = 0;
    if (!file_name.starts_with(prefix) || !isNumber(file_name.substr(prefix.size())) || !tryParse(version, file_name.substr(prefix.size())))
        return std::nullopt;
    return version;
}

/// Parses the Java `Properties.load` format. Returns nothing for a malformed `\uXXXX` escape.
std::optional<std::unordered_map<String, String>> parseJavaProperties(std::string_view content)
{
    auto is_space = [](char c) { return c == ' ' || c == '\t' || c == '\f'; };
    auto skip_spaces = [&](std::string_view s)
    {
        while (!s.empty() && is_space(s.front()))
            s.remove_prefix(1);
        return s;
    };

    /// Bytes are ISO-8859-1, so every byte and every `\uXXXX` unit is one character.
    bool malformed = false;
    auto unescape = [&](std::string_view s)
    {
        String result;
        auto append = [&](int code_point)
        {
            char buf[4];
            result.append(buf, UTF8::convertCodePointToUTF8(code_point, buf, sizeof(buf)));
        };
        for (size_t i = 0; i < s.size(); ++i)
        {
            UInt8 c = s[i];
            if (c == '\\')
            {
                if (++i == s.size())
                    break;
                c = s[i];
                if (c == 'u')
                {
                    if (s.size() - i <= 4 || !std::all_of(s.begin() + i + 1, s.begin() + i + 5, isHexDigit))
                    {
                        malformed = true;
                        return result;
                    }
                    append(unhex4(s.data() + i + 1));
                    i += 4;
                    continue;
                }
                if (size_t escape = std::string_view("tnrf").find(static_cast<char>(c)); escape != std::string_view::npos)
                    c = "\t\n\r\f"[escape];
            }
            append(c);
        }
        return result;
    };

    std::unordered_map<String, String> properties;
    auto parse_logical_line = [&](std::string_view line)
    {
        size_t key_end = 0;
        bool escaped = false;
        for (; key_end < line.size(); ++key_end)
        {
            char c = line[key_end];
            if (!escaped && (c == '=' || c == ':' || is_space(c)))
                break;
            escaped = c == '\\' && !escaped;
        }
        auto value = skip_spaces(line.substr(key_end));
        if (!value.empty() && (value.front() == '=' || value.front() == ':'))
            value.remove_prefix(1);
        auto key = unescape(line.substr(0, key_end));
        properties[std::move(key)] = unescape(skip_spaces(value));
    };

    String logical;
    for (size_t pos = 0; pos <= content.size();)
    {
        size_t end = std::min(content.find_first_of("\r\n", pos), content.size());
        auto line = skip_spaces(content.substr(pos, end - pos));
        pos = end + ((content.substr(end, 2) == "\r\n") ? 2 : 1);

        if (logical.empty() && (line.empty() || line.front() == '#' || line.front() == '!'))
            continue;
        logical += line;
        size_t backslashes = 0;
        while (backslashes < logical.size() && logical[logical.size() - 1 - backslashes] == '\\')
            ++backslashes;
        if (backslashes % 2 == 1)
        {
            logical.pop_back();
            continue;
        }
        parse_logical_line(logical);
        logical.clear();
    }
    if (!logical.empty())
        parse_logical_line(logical);

    if (malformed)
        return std::nullopt;
    return properties;
}

/// The timeline Hudi reads, chosen by `hoodie.timeline.layout.version` of `hoodie.properties`, else of its backup.
/// Nothing if neither is readable or the version is absent.
std::optional<ActiveTimeline> getActiveTimeline(
    std::optional<RelativePathWithMetadata> & properties_file,
    std::optional<RelativePathWithMetadata> & backup_file,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context,
    const LoggerPtr & log)
{
    std::optional<std::unordered_map<String, String>> properties;
    for (auto * file : {&properties_file, &backup_file})
    {
        if (!*file)
            continue;
        auto buf = createReadBuffer(**file, object_storage, context, log);
        String content;
        readStringUntilEOF(content, *buf);
        properties = parseJavaProperties(content);
        if (properties)
            break;
    }
    if (!properties)
        return std::nullopt;

    auto layout = properties->find("hoodie.timeline.layout.version");
    if (layout == properties->end())
        return std::nullopt;
    if (layout->second == "0" || layout->second == "1")
        return ActiveTimeline{".hoodie", false};
    if (layout->second != "2")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported Hudi timeline layout version {}", layout->second);

    if (auto path = properties->find("hoodie.timeline.path"); path != properties->end() && path->second != "timeline")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Reading a Hudi table with a custom hoodie.timeline.path ('{}') is not supported", path->second);
    return ActiveTimeline{".hoodie/timeline", true};
}

void collectReplacedFileGroups(
    RelativePathWithMetadata & instant,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context,
    const LoggerPtr & log,
    ReplacedFileGroups & replaced)
{
    /// Hudi reads an empty instant as empty metadata.
    if (instant.metadata && instant.metadata->is_size_known && instant.metadata->size_bytes == 0)
        return;

    size_t num_replaced = 0;
    auto add = [&](const String & partition, const String & file_id)
    {
        replaced[partition].insert(file_id);
        ++num_replaced;
    };
    auto incorrect_field = [&]
    {
        return Exception(ErrorCodes::INCORRECT_DATA, "Unexpected type of partitionToReplaceFileIds in Hudi instant {}", instant.relative_path);
    };

    /// Only a layout 2 instant with a completion time is Avro, the others are JSON.
    if (instant.getFileName().contains('_'))
    {
#if USE_AVRO
        /// NOLINTBEGIN(clang-analyzer-core.uninitialized.UndefReturn)
        /// A false positive on the `*std::any_cast` of `avro::GenericDatum::value`.
        Iceberg::forEachAvroEntry(
            instant.relative_path,
            object_storage,
            context,
            "HudiMetadata",
            [&](const avro::GenericDatum & datum)
            {
                const auto & record = datum.value<avro::GenericRecord>();
                if (!record.hasField("partitionToReplaceFileIds"))
                    return;
                const auto & field = record.field("partitionToReplaceFileIds");
                if (field.type() == avro::AVRO_NULL)
                    return;
                if (field.type() != avro::AVRO_MAP)
                    throw incorrect_field();
                for (const auto & [partition, file_ids] : field.value<avro::GenericMap>().value())
                {
                    if (file_ids.type() != avro::AVRO_ARRAY)
                        throw incorrect_field();
                    for (const auto & file_id : file_ids.value<avro::GenericArray>().value())
                    {
                        if (file_id.type() != avro::AVRO_STRING)
                            throw incorrect_field();
                        add(partition, file_id.value<std::string>());
                    }
                }
            });
        /// NOLINTEND(clang-analyzer-core.uninitialized.UndefReturn)
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED, "Cannot read Hudi instant {}: ClickHouse is built without Avro support", instant.relative_path);
#endif
    }
    else
    {
        auto buf = createReadBuffer(instant, object_storage, context, log);
        String content;
        readStringUntilEOF(content, *buf);
        Poco::JSON::Parser parser;
        auto json = parser.parse(content);
        if (json.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Hudi instant {} is not a JSON object", instant.relative_path);
        const auto & root = json.extract<Poco::JSON::Object::Ptr>();
        /// An absent or null field means that nothing is replaced.
        if (root->has("partitionToReplaceFileIds") && !root->isNull("partitionToReplaceFileIds"))
        {
            auto partitions = root->getObject("partitionToReplaceFileIds");
            if (!partitions)
                throw incorrect_field();
            for (const auto & [partition, file_ids] : *partitions)
            {
                if (file_ids.type() != typeid(Poco::JSON::Array::Ptr))
                    throw incorrect_field();
                for (const auto & file_id : *file_ids.extract<Poco::JSON::Array::Ptr>())
                {
                    if (!file_id.isString())
                        throw incorrect_field();
                    add(partition, file_id.extract<String>());
                }
            }
        }
    }

    LOG_TRACE(log, "Hudi instant {} replaces {} file groups", instant.relative_path, num_replaced);
}

/// The files of the latest snapshot of the timeline history: the manifest named by `_version_`, else the newest one, as
/// Hudi reads it. The other files in `history_path` are superseded by a compaction of the history or not committed.
Strings readHistorySnapshot(
    const String & history_path,
    bool has_version_file,
    std::optional<UInt64> newest_manifest,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context,
    const LoggerPtr & log)
{
    std::optional<UInt64> version;
    if (has_version_file)
    {
        /// Without the listed size, which is stale if Hudi has rewritten the file since.
        RelativePathWithMetadata version_file(history_path + "_version_");
        auto buf = createReadBuffer(version_file, object_storage, context, log);
        String content;
        readStringUntilEOF(content, *buf);
        if (UInt64 parsed = 0; isNumber(content) && tryParse(parsed, content))
            version = parsed;
    }
    if (!version)
        version = newest_manifest;
    if (!version)
        return {};

    RelativePathWithMetadata manifest_file(fmt::format("{}manifest_{}", history_path, *version));
    auto buf = createReadBuffer(manifest_file, object_storage, context, log);
    String content;
    readStringUntilEOF(content, *buf);
    /// Hudi reads an empty manifest as one without files.
    if (content.empty())
        return {};

    auto incorrect_manifest = [&]
    {
        return Exception(
            ErrorCodes::INCORRECT_DATA, "Unexpected format of the Hudi timeline history manifest {}", manifest_file.relative_path);
    };
    Poco::JSON::Parser parser;
    auto json = parser.parse(content);
    if (json.type() != typeid(Poco::JSON::Object::Ptr))
        throw incorrect_manifest();
    const auto & root = json.extract<Poco::JSON::Object::Ptr>();
    if (!root->has("files"))
        return {};
    auto files = root->getArray("files");
    if (!files)
        throw incorrect_manifest();
    Strings file_names;
    for (const auto & file : *files)
    {
        if (file.type() != typeid(Poco::JSON::Object::Ptr))
            throw incorrect_manifest();
        auto file_name = file.extract<Poco::JSON::Object::Ptr>()->get("fileName");
        if (!file_name.isString())
            throw incorrect_manifest();
        file_names.push_back(file_name.extract<String>());
    }
    return file_names;
}

/// Adds the completion times of `instants` recorded in `history_files` of `history_path` to `completion_times`.
void readArchivedCompletionTimes(
    const String & history_path,
    const Strings & history_files,
    const std::unordered_set<UInt64> & instants,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context,
    const LoggerPtr & log,
    std::unordered_map<UInt64, UInt64> & completion_times)
{
#if USE_PARQUET
    auto read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.random_access = FormatFactory::instance().checkIfFormatIsRandomAccessInput("Parquet", context);
    auto format_settings = getFormatSettings(context);
    format_settings.parquet.allow_missing_columns = true;
    const auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    const Block header{ColumnWithTypeAndName(type, "instantTime"), ColumnWithTypeAndName(type, "completionTime")};

    for (const auto & file_name : history_files)
    {
        auto range = parseHistoryFileName(file_name);
        UInt64 min_time = 0;
        UInt64 max_time = 0;
        if (!range || !tryParse(min_time, range->first) || !tryParse(max_time, range->second))
            continue;
        if (std::none_of(instants.begin(), instants.end(), [&](UInt64 instant) { return min_time <= instant && instant <= max_time; }))
            continue;

        RelativePathWithMetadata file(history_path + file_name);
        auto buf = createReadBuffer(file, object_storage, context, log, read_settings);
        auto input = FormatFactory::instance().getInput("Parquet", *buf, header, context, DEFAULT_BLOCK_SIZE, format_settings);
        QueryPipeline pipeline(std::move(input));
        PullingPipelineExecutor executor(pipeline);
        Block block;
        while (executor.pull(block))
        {
            const auto & requested_column = *block.getByName("instantTime").column;
            const auto & completion_column = *block.getByName("completionTime").column;
            for (size_t row = 0; row < block.rows(); ++row)
            {
                UInt64 requested = 0;
                UInt64 completion = 0;
                if (!requested_column.isNullAt(row) && !completion_column.isNullAt(row)
                    && tryParse(requested, requested_column.getDataAt(row)) && tryParse(completion, completion_column.getDataAt(row))
                    && instants.contains(requested))
                    completion_times.emplace(requested, completion);
            }
        }
    }
#else
    UNUSED(history_path, history_files, instants, object_storage, context, log, completion_times);
#endif
}

}

/**
    * Useful links:
    * - https://hudi.apache.org/tech-specs/
    * - https://hudi.apache.org/docs/storage_layouts
    */

/**
    * Hudi tables store metadata files and data files.
    * Metadata files are stored in .hoodie/metadata directory. Though unlike DeltaLake and Iceberg,
    * metadata is not required in order to understand which files we need to read, moreover,
    * for Hudi metadata does not always exist.
    *
    * There can be two types of data files
    * 1. base files (columnar file formats like Apache Parquet/Orc)
    * 2. log files
    * Currently we support reading only `base files`, so a table whose latest file slices have log files is refused.
    * Data file name format:
    * [File Id]_[File Write Token]_[Transaction timestamp].[File Extension]
    *
    * To find needed parts we need to find out latest part file for every file group for every partition.
    * Explanation why:
    *    Hudi reads in and overwrites the entire table/partition with each update.
    *    Hudi controls the number of file groups under a single partition according to the
    *    hoodie.parquet.max.file.size option. Once a single Parquet file is too large, Hudi creates a second file group.
    *    Each file group is identified by File Id.
    */
Strings HudiMetadata::getDataFilesImpl(const ContextPtr & local_context) const
{
    auto log = getLogger("HudiMetadata");
    FailPointInjection::pauseFailPoint(FailPoints::hudi_pause_in_listing_data_files);

    /// `table_path` is "" for a table at the root of a bucket, and ends with '/' otherwise.
    const String data_file_suffix = Poco::toLower(format);
    std::optional<RelativePathWithMetadata> properties_file;
    std::optional<RelativePathWithMetadata> backup_file;
    std::vector<RelativePathWithMetadata> instants;
    std::vector<String> timeline_files;
    bool history_has_version_file = false;
    std::optional<UInt64> newest_history_manifest;
    /// Listed before the data files, so the new files of every instant applied below are listed too.
    listFiles(
        *object_storage,
        table_path,
        ".hoodie/",
        [&](const RelativePathWithMetadata & object)
        {
            const String & key = object.relative_path;
            if (!key.starts_with(table_path))
                return false;
            std::string_view path_in_table = std::string_view(key).substr(table_path.size());
            std::string_view directory = directoryInTable(key, table_path);
            if (directory == ".hoodie/timeline")
                timeline_files.push_back(object.getFileName());
            else if (directory == ".hoodie/timeline/history")
            {
                const String file_name = object.getFileName();
                if (file_name == "_version_")
                    history_has_version_file = true;
                else if (auto version = parseManifestVersion(file_name))
                    newest_history_manifest = std::max(newest_history_manifest.value_or(0), *version);
            }
            if (path_in_table == ".hoodie/hoodie.properties")
                properties_file.emplace(object);
            else if (path_in_table == ".hoodie/hoodie.properties.backup")
                backup_file.emplace(object);
            else if (path_in_table.starts_with(".hoodie/") && path_in_table.ends_with(".replacecommit"))
                instants.push_back(object);
            return false;
        });

    /// A completed `replacecommit` (insert_overwrite, clustering) removes file groups, whose files stay on
    /// storage until the cleaner deletes them.
    std::optional<ActiveTimeline> timeline;
    if (properties_file || backup_file)
        timeline = getActiveTimeline(properties_file, backup_file, object_storage, local_context, log);
    if (!timeline && !instants.empty())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Hudi table {} has timeline instants but no readable hoodie.timeline.layout.version in .hoodie/hoodie.properties",
            table_path);

    ReplacedFileGroups replaced;
    for (auto & instant : instants)
    {
        if (directoryInTable(instant.relative_path, table_path) != timeline->directory)
            continue;
        const String file_name = instant.getFileName();
        auto completed = parseCompletedInstantName(file_name, timeline->layout_2);
        if (completed && completed->action == "replacecommit")
            collectReplacedFileGroups(instant, object_storage, local_context, log, replaced);
    }

    /// Requested time -> completion time of the completed instants of a layout 2 timeline.
    std::unordered_map<UInt64, UInt64> completion_times;
    if (timeline && timeline->layout_2)
    {
        for (const auto & file_name : timeline_files)
        {
            auto completed = parseCompletedInstantName(file_name, true);
            UInt64 requested = 0;
            UInt64 completion = 0;
            if (completed && tryParse(requested, completed->requested) && tryParse(completion, completed->completion))
                completion_times.emplace(requested, completion);
        }
    }

    FailPointInjection::pauseFailPoint(FailPoints::hudi_pause_after_listing_timeline);

    const auto keys = listFiles(
        *object_storage,
        table_path,
        "",
        [&](const RelativePathWithMetadata & object)
        {
            const String & key = object.relative_path;
            /// Nothing under `.hoodie/` is a data file of the table.
            if (key.starts_with(table_path) && std::string_view(key).substr(table_path.size()).starts_with(".hoodie/"))
                return false;
            return key.ends_with(data_file_suffix) || parseLogFileName(object.getFileName());
        });

    using Partition = std::string;
    using FileID = std::string;
    struct FileInfo
    {
        String key;
        UInt64 timestamp = 0;
    };
    std::unordered_map<Partition, std::unordered_map<FileID, FileInfo>> files;

    struct LogFileInfo
    {
        Partition partition;
        FileID file_id;
        std::optional<UInt64> instant;
        String key;
    };
    std::vector<LogFileInfo> log_files;
    std::unordered_set<UInt64> unknown_completions;

    for (const auto & key : keys)
    {
        auto key_file = std::filesystem::path(key);
        /// Hudi identifies a file group by the partition path relative to the table root and the file id.
        const String partition(directoryInTable(key, table_path));

        const String file_name = key_file.filename();
        if (auto log_file = parseLogFileName(file_name))
        {
            const auto & [log_file_id, instant] = *log_file;
            if (auto it = replaced.find(partition); it != replaced.end() && it->second.contains(String(log_file_id)))
                continue;

            std::optional<UInt64> log_instant;
            if (UInt64 parsed = 0; timeline && tryParse(parsed, instant))
                log_instant = parsed;
            if (timeline && timeline->layout_2 && log_instant && !completion_times.contains(*log_instant))
                unknown_completions.insert(*log_instant);
            log_files.push_back({partition, String(log_file_id), log_instant, key});
            continue;
        }

        Strings file_parts;
        const String stem = key_file.stem();
        splitInto<'_'>(file_parts, stem);
        if (file_parts.size() != 3)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unexpected format for file: {}. Hudi data files must follow the naming convention "
                "[FileId]_[FileWriteToken]_[Timestamp].[extension]", key);

        const auto & file_id = file_parts[0];
        const auto timestamp = parse<UInt64>(file_parts[2]);
        if (auto it = replaced.find(partition); it != replaced.end() && it->second.contains(file_id))
            continue;

        auto & file_info = files[partition][file_id];
        if (file_info.timestamp == 0 || file_info.timestamp < timestamp)
        {
            file_info.key = key;
            file_info.timestamp = timestamp;
        }
    }

    if (!unknown_completions.empty())
    {
        const String history_path = table_path + ".hoodie/timeline/history/";
        const auto history_files
            = readHistorySnapshot(history_path, history_has_version_file, newest_history_manifest, object_storage, local_context, log);
        readArchivedCompletionTimes(history_path, history_files, unknown_completions, object_storage, local_context, log, completion_times);
    }

    for (const auto & log_file : log_files)
    {
        /// The instant of a log is the base instant of its file slice in layout 0 and 1, and the requested time of its
        /// delta commit in layout 2, where the log belongs to the last file slice that started before the commit completed.
        UInt64 time = std::numeric_limits<UInt64>::max();
        if (timeline && log_file.instant)
        {
            if (!timeline->layout_2)
                time = *log_file.instant;
            else if (auto it = completion_times.find(*log_file.instant); it != completion_times.end())
                time = it->second;
        }

        const FileInfo * base_file = nullptr;
        if (auto partition_it = files.find(log_file.partition); partition_it != files.end())
            if (auto it = partition_it->second.find(log_file.file_id); it != partition_it->second.end())
                base_file = &it->second;
        if (!base_file || time >= base_file->timestamp)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Reading a Hudi table with log files that are not compacted is not supported: the changes in '{}' would be "
                "missing from the result, because the log files of Merge-on-Read tables are not read. Compact the table in "
                "Hudi to read it",
                log_file.key);
    }

    Strings result;
    for (auto & [partition, partition_data] : files)
    {
        LOG_TRACE(log, "Adding {} data files from partition {}", partition, partition_data.size());
        for (auto & [file_id, file_data] : partition_data)
            result.push_back(std::move(file_data.key));
    }
    return result;
}

HudiMetadata::HudiMetadata(ObjectStoragePtr object_storage_, StorageObjectStorageConfigurationPtr configuration_, ContextPtr context_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , table_path(configuration_->getPathForRead().path)
    , format(configuration_->format)
{
}

Strings HudiMetadata::getDataFiles(const ActionsDAG *, const ContextPtr & local_context) const
{
    std::lock_guard lock(data_files_mutex);
    if (data_files.empty())
        data_files = getDataFilesImpl(local_context);
    return data_files;
}

void HudiMetadata::pauseBeforeIterate()
{
    FailPointInjection::pauseFailPoint(FailPoints::hudi_pause_before_iterate);
}

Strings HudiMetadata::getDataFilesIfListed() const
{
    std::lock_guard lock(data_files_mutex);
    return data_files;
}

ObjectIterator HudiMetadata::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t /* list_batch_size */,
    StorageMetadataPtr /* storage_metadata_snapshot*/,
    ContextPtr local_context) const
{
    return createKeysIterator(getDataFiles(filter_dag, local_context), object_storage, callback);
}

}
