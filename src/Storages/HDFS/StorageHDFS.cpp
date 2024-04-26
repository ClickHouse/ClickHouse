#include "config.h"

#if USE_HDFS

#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <DataTypes/DataTypeString.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <IO/WriteHelpers.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteSettings.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Storages/StorageFactory.h>
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Formats/ReadSchemaUtils.h>
#include <Formats/FormatFactory.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>

#include <Poco/URI.h>
#include <hdfs/hdfs.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event EngineFileLikeReadFiles;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ACCESS_DENIED;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int CANNOT_DETECT_FORMAT;
}
namespace
{
    struct HDFSFileInfoDeleter
    {
        /// Can have only one entry (see hdfsGetPathInfo())
        void operator()(hdfsFileInfo * info) { hdfsFreeFileInfo(info, 1); }
    };
    using HDFSFileInfoPtr = std::unique_ptr<hdfsFileInfo, HDFSFileInfoDeleter>;

    /* Recursive directory listing with matched paths as a result.
     * Have the same method in StorageFile.
     */
    std::vector<StorageHDFS::PathWithInfo> LSWithRegexpMatching(
        const String & path_for_ls,
        const HDFSFSPtr & fs,
        const String & for_match)
    {
        std::vector<StorageHDFS::PathWithInfo> result;

        const size_t first_glob_pos = for_match.find_first_of("*?{");

        if (first_glob_pos == std::string::npos)
        {
            const String path = fs::path(path_for_ls + for_match.substr(1)).lexically_normal();
            HDFSFileInfoPtr hdfs_info(hdfsGetPathInfo(fs.get(), path.c_str()));
            if (hdfs_info) // NOLINT
            {
                result.push_back(StorageHDFS::PathWithInfo{
                        String(path),
                        StorageHDFS::PathInfo{hdfs_info->mLastMod, static_cast<size_t>(hdfs_info->mSize)}});
            }
            return result;
        }

        const size_t end_of_path_without_globs = for_match.substr(0, first_glob_pos).rfind('/');
        const String suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'
        const String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'

        const size_t next_slash_after_glob_pos = suffix_with_globs.find('/', 1);

        const std::string current_glob = suffix_with_globs.substr(0, next_slash_after_glob_pos);

        re2::RE2 matcher(makeRegexpPatternFromGlobs(current_glob));
        if (!matcher.ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regex from glob ({}): {}", for_match, matcher.error());

        HDFSFileInfo ls;
        ls.file_info = hdfsListDirectory(fs.get(), prefix_without_globs.data(), &ls.length);
        if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
        {
            // ignore file not found exception, keep throw other exception, libhdfs3 doesn't have function to get exception type, so use errno.
            throw Exception(
                ErrorCodes::ACCESS_DENIED, "Cannot list directory {}: {}", prefix_without_globs, String(hdfsGetLastError()));
        }

        if (!ls.file_info && ls.length > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "file_info shouldn't be null");
        for (int i = 0; i < ls.length; ++i)
        {
            const String full_path = fs::path(ls.file_info[i].mName).lexically_normal();
            const size_t last_slash = full_path.rfind('/');
            const String file_name = full_path.substr(last_slash);
            const bool looking_for_directory = next_slash_after_glob_pos != std::string::npos;
            const bool is_directory = ls.file_info[i].mKind == 'D';
            /// Condition with type of current file_info means what kind of path is it in current iteration of ls
            if (!is_directory && !looking_for_directory)
            {
                if (re2::RE2::FullMatch(file_name, matcher))
                    result.push_back(StorageHDFS::PathWithInfo{
                        String(full_path),
                        StorageHDFS::PathInfo{ls.file_info[i].mLastMod, static_cast<size_t>(ls.file_info[i].mSize)}});
            }
            else if (is_directory && looking_for_directory)
            {
                if (re2::RE2::FullMatch(file_name, matcher))
                {
                    std::vector<StorageHDFS::PathWithInfo> result_part = LSWithRegexpMatching(fs::path(full_path) / "", fs,
                        suffix_with_globs.substr(next_slash_after_glob_pos));
                    /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                    std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
                }
            }
        }

        return result;
    }

    std::pair<String, String> getPathFromUriAndUriWithoutPath(const String & uri)
    {
        auto pos = uri.find("//");
        if (pos != std::string::npos && pos + 2 < uri.length())
        {
            pos = uri.find('/', pos + 2);
            if (pos != std::string::npos)
                return {uri.substr(pos), uri.substr(0, pos)};
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage HDFS requires valid URL to be set");
    }

    std::vector<StorageHDFS::PathWithInfo> getPathsList(const String & path_from_uri, const String & uri_without_path, ContextPtr context)
    {
        HDFSBuilderWrapper builder = createHDFSBuilder(uri_without_path + "/", context->getGlobalContext()->getConfigRef());
        HDFSFSPtr fs = createHDFSFS(builder.get());

        Strings paths = expandSelectionGlob(path_from_uri);

        std::vector<StorageHDFS::PathWithInfo> res;

        for (const auto & path : paths)
        {
            auto part_of_res = LSWithRegexpMatching("/", fs, path);
            res.insert(res.end(), part_of_res.begin(), part_of_res.end());
        }
        return res;
    }
}

StorageHDFS::StorageHDFS(
    const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const ContextPtr & context_,
    const String & compression_method_,
    const bool distributed_processing_,
    ASTPtr partition_by_)
    : IStorage(table_id_)
    , WithContext(context_)
    , uris({uri_})
    , format_name(format_name_)
    , compression_method(compression_method_)
    , distributed_processing(distributed_processing_)
    , partition_by(partition_by_)
{
    if (format_name != "auto")
        FormatFactory::instance().checkFormatName(format_name);
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri_));
    checkHDFSURL(uri_);

    String path = uri_.substr(uri_.find('/', uri_.find("//") + 2));
    is_path_with_globs = path.find_first_of("*?{") != std::string::npos;

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        ColumnsDescription columns;
        if (format_name == "auto")
            std::tie(columns, format_name) = getTableStructureAndFormatFromData(uri_, compression_method_, context_);
        else
            columns = getTableStructureFromData(format_name, uri_, compression_method, context_);

        storage_metadata.setColumns(columns);
    }
    else
    {
        if (format_name == "auto")
            format_name = getTableStructureAndFormatFromData(uri_, compression_method_, context_).second;

        /// We don't allow special columns in HDFS storage.
        if (!columns_.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table engine HDFS doesn't support special columns like MATERIALIZED, ALIAS or EPHEMERAL");
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));
}

namespace
{
    class ReadBufferIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferIterator(
            const std::vector<StorageHDFS::PathWithInfo> & paths_with_info_,
            const String & uri_without_path_,
            std::optional<String> format_,
            const String & compression_method_,
            const ContextPtr & context_)
        : WithContext(context_)
        , paths_with_info(paths_with_info_)
        , uri_without_path(uri_without_path_)
        , format(std::move(format_))
        , compression_method(compression_method_)
        {
        }

        Data next() override
        {
            bool is_first = current_index == 0;
            /// For default mode check cached columns for all paths on first iteration.
            if (is_first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
            {
                if (auto cached_columns = tryGetColumnsFromCache(paths_with_info))
                    return {nullptr, cached_columns, format};
            }

            StorageHDFS::PathWithInfo path_with_info;

            while (true)
            {
                if (current_index == paths_with_info.size())
                {
                    if (is_first)
                    {
                        if (format)
                            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                                            "The table structure cannot be extracted from a {} format file, because all files are empty. "
                                            "You can specify table structure manually", *format);

                        throw Exception(
                            ErrorCodes::CANNOT_DETECT_FORMAT,
                            "The data format cannot be detected by the contents of the files, because all files are empty. You can specify table structure manually");
                    }
                    return {nullptr, std::nullopt, format};
                }

                path_with_info = paths_with_info[current_index++];
                if (getContext()->getSettingsRef().hdfs_skip_empty_files && path_with_info.info && path_with_info.info->size == 0)
                    continue;

                if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
                {
                    std::vector<StorageHDFS::PathWithInfo> paths = {path_with_info};
                    if (auto cached_columns = tryGetColumnsFromCache(paths))
                        return {nullptr, cached_columns, format};
                }

                auto compression = chooseCompressionMethod(path_with_info.path, compression_method);
                auto impl = std::make_unique<ReadBufferFromHDFS>(uri_without_path, path_with_info.path, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
                if (!getContext()->getSettingsRef().hdfs_skip_empty_files || !impl->eof())
                {
                    const Int64 zstd_window_log_max = getContext()->getSettingsRef().zstd_window_log_max;
                    return {wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max)), std::nullopt, format};
                }
            }
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_hdfs)
                return;

            String source = uri_without_path + paths_with_info[current_index - 1].path;
            auto key = getKeyForSchemaCache(source, *format, std::nullopt, getContext());
            StorageHDFS::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

        void setSchemaToLastFile(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_hdfs
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::UNION)
                return;

            String source = uri_without_path + paths_with_info[current_index - 1].path;
            auto key = getKeyForSchemaCache(source, *format, std::nullopt, getContext());
            StorageHDFS::getSchemaCache(getContext()).addColumns(key, columns);
        }

        void setResultingSchema(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_hdfs
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::DEFAULT)
                return;

            Strings sources;
            sources.reserve(paths_with_info.size());
            std::transform(paths_with_info.begin(), paths_with_info.end(), std::back_inserter(sources), [&](const StorageHDFS::PathWithInfo & path_with_info){ return uri_without_path + path_with_info.path; });
            auto cache_keys = getKeysForSchemaCache(sources, *format, {}, getContext());
            StorageHDFS::getSchemaCache(getContext()).addManyColumns(cache_keys, columns);
        }

        void setFormatName(const String & format_name) override
        {
            format = format_name;
        }

        String getLastFileName() const override
        {
            if (current_index != 0)
                return paths_with_info[current_index - 1].path;

            return "";
        }

        bool supportsLastReadBufferRecreation() const override { return true; }

        std::unique_ptr<ReadBuffer> recreateLastReadBuffer() override
        {
            chassert(current_index > 0 && current_index <= paths_with_info.size());
            auto path_with_info = paths_with_info[current_index - 1];
            auto compression = chooseCompressionMethod(path_with_info.path, compression_method);
            auto impl = std::make_unique<ReadBufferFromHDFS>(uri_without_path, path_with_info.path, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
            const Int64 zstd_window_log_max = getContext()->getSettingsRef().zstd_window_log_max;
            return wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max));
        }

    private:
        std::optional<ColumnsDescription> tryGetColumnsFromCache(const std::vector<StorageHDFS::PathWithInfo> & paths_with_info_)
        {
            auto context = getContext();

            if (!context->getSettingsRef().schema_inference_use_cache_for_hdfs)
                return std::nullopt;

            auto & schema_cache = StorageHDFS::getSchemaCache(context);
            for (const auto & path_with_info : paths_with_info_)
            {
                auto get_last_mod_time = [&]() -> std::optional<time_t>
                {
                    if (path_with_info.info)
                        return path_with_info.info->last_mod_time;

                    auto builder = createHDFSBuilder(uri_without_path + "/", context->getGlobalContext()->getConfigRef());
                    auto fs = createHDFSFS(builder.get());
                    HDFSFileInfoPtr hdfs_info(hdfsGetPathInfo(fs.get(), path_with_info.path.c_str()));
                    if (hdfs_info)
                        return hdfs_info->mLastMod;

                    return std::nullopt;
                };

                String url = uri_without_path + path_with_info.path;
                if (format)
                {
                    auto cache_key = getKeyForSchemaCache(url, *format, {}, context);
                    if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                        return columns;
                }
                else
                {
                    /// If format is unknown, we can iterate through all possible input formats
                    /// and check if we have an entry with this format and this file in schema cache.
                    /// If we have such entry for some format, we can use this format to read the file.
                    for (const auto & format_name : FormatFactory::instance().getAllInputFormats())
                    {
                        auto cache_key = getKeyForSchemaCache(url, format_name, {}, context);
                        if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                        {
                            /// Now format is known. It should be the same for all files.
                            format = format_name;
                            return columns;
                        }
                    }
                }
            }

            return std::nullopt;
        }

        const std::vector<StorageHDFS::PathWithInfo> & paths_with_info;
        const String & uri_without_path;
        std::optional<String> format;
        const String & compression_method;
        size_t current_index = 0;
    };
}

std::pair<ColumnsDescription, String> StorageHDFS::getTableStructureAndFormatFromDataImpl(
    std::optional<String> format,
    const String & uri,
    const String & compression_method,
    const ContextPtr & ctx)
{
    const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);
    auto paths_with_info = getPathsList(path_from_uri, uri, ctx);

    if (paths_with_info.empty() && (!format || !FormatFactory::instance().checkIfFormatHasExternalSchemaReader(*format)))
    {
        if (format)
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "The table structure cannot be extracted from a {} format file, because there are no files in HDFS with provided path."
                " You can specify table structure manually", *format);

        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "The data format cannot be detected by the contents of the files, because there are no files in HDFS with provided path."
            " You can specify the format manually");
    }

    ReadBufferIterator read_buffer_iterator(paths_with_info, uri_without_path, format, compression_method, ctx);
    if (format)
        return {readSchemaFromFormat(*format, std::nullopt, read_buffer_iterator, ctx), *format};
    return detectFormatAndReadSchema(std::nullopt, read_buffer_iterator, ctx);
}

std::pair<ColumnsDescription, String> StorageHDFS::getTableStructureAndFormatFromData(const String & uri, const String & compression_method, const ContextPtr & ctx)
{
    return getTableStructureAndFormatFromDataImpl(std::nullopt, uri, compression_method, ctx);
}

ColumnsDescription StorageHDFS::getTableStructureFromData(const String & format, const String & uri, const String & compression_method, const DB::ContextPtr & ctx)
{
    return getTableStructureAndFormatFromDataImpl(format, uri, compression_method, ctx).first;
}

class HDFSSource::DisclosedGlobIterator::Impl
{
public:
    Impl(const String & uri, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context)
    {
        const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);
        uris = getPathsList(path_from_uri, uri_without_path, context);
        ActionsDAGPtr filter_dag;
        if (!uris.empty())
             filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);

        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(uris.size());
            for (const auto & path_with_info : uris)
                paths.push_back(path_with_info.path);

            VirtualColumnUtils::filterByPathOrFile(uris, paths, filter_dag, virtual_columns, context);
        }
        auto file_progress_callback = context->getFileProgressCallback();

        for (auto & elem : uris)
        {
            elem.path = uri_without_path + elem.path;
            if (file_progress_callback && elem.info)
                file_progress_callback(FileProgress(0, elem.info->size));
        }
        uris_iter = uris.begin();
    }

    StorageHDFS::PathWithInfo next()
    {
        std::lock_guard lock(mutex);
        if (uris_iter != uris.end())
        {
            auto answer = *uris_iter;
            ++uris_iter;
            return answer;
        }
        return {};
    }
private:
    std::mutex mutex;
    std::vector<StorageHDFS::PathWithInfo> uris;
    std::vector<StorageHDFS::PathWithInfo>::iterator uris_iter;
};

class HDFSSource::URISIterator::Impl : WithContext
{
public:
    explicit Impl(const std::vector<String> & uris_, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context_)
        : WithContext(context_), uris(uris_), file_progress_callback(context_->getFileProgressCallback())
    {
        ActionsDAGPtr filter_dag;
        if (!uris.empty())
            filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);

        if (filter_dag)
        {
            std::vector<String> paths;
            paths.reserve(uris.size());
            for (const auto & uri : uris)
                paths.push_back(getPathFromUriAndUriWithoutPath(uri).first);

            VirtualColumnUtils::filterByPathOrFile(uris, paths, filter_dag, virtual_columns, getContext());
        }

        if (!uris.empty())
        {
            auto path_and_uri = getPathFromUriAndUriWithoutPath(uris[0]);
            builder = createHDFSBuilder(path_and_uri.second + "/", getContext()->getGlobalContext()->getConfigRef());
            fs = createHDFSFS(builder.get());
        }
    }

    StorageHDFS::PathWithInfo next()
    {
        String uri;
        HDFSFileInfoPtr hdfs_info;
        do
        {
            size_t current_index = index.fetch_add(1);
            if (current_index >= uris.size())
                return {"", {}};

            uri = uris[current_index];
            auto path_and_uri = getPathFromUriAndUriWithoutPath(uri);
            hdfs_info.reset(hdfsGetPathInfo(fs.get(), path_and_uri.first.c_str()));
        }
        /// Skip non-existed files.
        while (!hdfs_info && String(hdfsGetLastError()).find("FileNotFoundException") != std::string::npos);

        std::optional<StorageHDFS::PathInfo> info;
        if (hdfs_info)
        {
            info = StorageHDFS::PathInfo{hdfs_info->mLastMod, static_cast<size_t>(hdfs_info->mSize)};
            if (file_progress_callback)
                file_progress_callback(FileProgress(0, hdfs_info->mSize));
        }

        return {uri, info};
    }

private:
    std::atomic_size_t index = 0;
    Strings uris;
    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;
    std::function<void(FileProgress)> file_progress_callback;
};

HDFSSource::DisclosedGlobIterator::DisclosedGlobIterator(const String & uri, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context)
    : pimpl(std::make_shared<HDFSSource::DisclosedGlobIterator::Impl>(uri, predicate, virtual_columns, context)) {}

StorageHDFS::PathWithInfo HDFSSource::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

HDFSSource::URISIterator::URISIterator(const std::vector<String> & uris_, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context)
    : pimpl(std::make_shared<HDFSSource::URISIterator::Impl>(uris_, predicate, virtual_columns, context))
{
}

StorageHDFS::PathWithInfo HDFSSource::URISIterator::next()
{
    return pimpl->next();
}

HDFSSource::HDFSSource(
    const ReadFromFormatInfo & info,
    StorageHDFSPtr storage_,
    const ContextPtr & context_,
    UInt64 max_block_size_,
    std::shared_ptr<IteratorWrapper> file_iterator_,
    bool need_only_count_)
    : ISource(info.source_header, false)
    , WithContext(context_)
    , storage(std::move(storage_))
    , block_for_format(info.format_header)
    , requested_columns(info.requested_columns)
    , requested_virtual_columns(info.requested_virtual_columns)
    , max_block_size(max_block_size_)
    , file_iterator(file_iterator_)
    , columns_description(info.columns_description)
    , need_only_count(need_only_count_)
{
    initialize();
}

HDFSSource::~HDFSSource() = default;

bool HDFSSource::initialize()
{
    bool skip_empty_files = getContext()->getSettingsRef().hdfs_skip_empty_files;
    StorageHDFS::PathWithInfo path_with_info;
    while (true)
    {
        path_with_info = (*file_iterator)();
        if (path_with_info.path.empty())
            return false;

        if (path_with_info.info && skip_empty_files && path_with_info.info->size == 0)
            continue;

        current_path = path_with_info.path;
        const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(current_path);

        std::optional<size_t> file_size;
        if (!path_with_info.info)
        {
            auto builder = createHDFSBuilder(uri_without_path + "/", getContext()->getGlobalContext()->getConfigRef());
            auto fs = createHDFSFS(builder.get());
            HDFSFileInfoPtr hdfs_info(hdfsGetPathInfo(fs.get(), path_from_uri.c_str()));
            if (hdfs_info)
                path_with_info.info = StorageHDFS::PathInfo{hdfs_info->mLastMod, static_cast<size_t>(hdfs_info->mSize)};
        }

        if (path_with_info.info)
            file_size = path_with_info.info->size;

        auto compression = chooseCompressionMethod(path_from_uri, storage->compression_method);
        auto impl = std::make_unique<ReadBufferFromHDFS>(
            uri_without_path, path_from_uri, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings(), 0, false, file_size);
        if (!skip_empty_files || !impl->eof())
        {
            impl->setProgressCallback(getContext());
            const Int64 zstd_window_log_max = getContext()->getSettingsRef().zstd_window_log_max;
            read_buf = wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max));
            break;
        }
    }

    current_path = path_with_info.path;
    current_file_size = path_with_info.info ? std::optional(path_with_info.info->size) : std::nullopt;

    QueryPipelineBuilder builder;
    std::optional<size_t> num_rows_from_cache = need_only_count && getContext()->getSettingsRef().use_cache_for_count_from_files ? tryGetNumRowsFromCache(path_with_info) : std::nullopt;
    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use a special ConstChunkGenerator that will generate chunks
        /// with max_block_size rows until total number of rows is reached.
        auto source = std::make_shared<ConstChunkGenerator>(block_for_format, *num_rows_from_cache, max_block_size);
        builder.init(Pipe(source));
    }
    else
    {
        std::optional<size_t> max_parsing_threads;
        if (need_only_count)
            max_parsing_threads = 1;

        input_format = getContext()->getInputFormat(storage->format_name, *read_buf, block_for_format, max_block_size, std::nullopt, max_parsing_threads);

        if (need_only_count)
            input_format->needOnlyCount();

        builder.init(Pipe(input_format));
        if (columns_description.hasDefaults())
        {
            builder.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<AddingDefaultsTransform>(header, columns_description, *input_format, getContext());
            });
        }
    }

    /// Add ExtractColumnsTransform to extract requested columns/subcolumns
    /// from the chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
    });

    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);
    return true;
}

String HDFSSource::getName() const
{
    return "HDFSSource";
}

Chunk HDFSSource::generate()
{
    while (true)
    {
        if (isCancelled() || !reader)
        {
            if (reader)
                reader->cancel();
            break;
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            UInt64 num_rows = chunk.getNumRows();
            total_rows_in_file += num_rows;
            size_t chunk_size = 0;
            if (input_format)
                chunk_size = input_format->getApproxBytesReadForChunk();
            progress(num_rows, chunk_size ? chunk_size : chunk.bytes());
            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(chunk, requested_virtual_columns, current_path, current_file_size);
            return chunk;
        }

        if (input_format && getContext()->getSettingsRef().use_cache_for_count_from_files)
            addNumRowsToCache(current_path, total_rows_in_file);

        total_rows_in_file = 0;

        reader.reset();
        pipeline.reset();
        input_format.reset();
        read_buf.reset();

        if (!initialize())
            break;
    }
    return {};
}

void HDFSSource::addNumRowsToCache(const String & path, size_t num_rows)
{
    auto cache_key = getKeyForSchemaCache(path, storage->format_name, std::nullopt, getContext());
    StorageHDFS::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

std::optional<size_t> HDFSSource::tryGetNumRowsFromCache(const StorageHDFS::PathWithInfo & path_with_info)
{
    auto cache_key = getKeyForSchemaCache(path_with_info.path, storage->format_name, std::nullopt, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        if (path_with_info.info)
            return path_with_info.info->last_mod_time;
        return std::nullopt;
    };

    return StorageHDFS::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
}

class HDFSSink : public SinkToStorage
{
public:
    HDFSSink(const String & uri,
        const String & format,
        const Block & sample_block,
        const ContextPtr & context,
        const CompressionMethod compression_method)
        : SinkToStorage(sample_block)
    {
        const auto & settings = context->getSettingsRef();
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromHDFS>(
                uri, context->getGlobalContext()->getConfigRef(), context->getSettingsRef().hdfs_replication, context->getWriteSettings()),
            compression_method,
            static_cast<int>(settings.output_format_compression_level),
            static_cast<int>(settings.output_format_compression_zstd_window_log));
        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context);
    }

    String getName() const override { return "HDFSSink"; }

    void consume(Chunk chunk) override
    {
        std::lock_guard lock(cancel_mutex);
        if (cancelled)
            return;
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onCancel() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
        cancelled = true;
    }

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization
            release();
        }
    }

    void onFinish() override
    {
        std::lock_guard lock(cancel_mutex);
        finalize();
    }

private:
    void finalize()
    {
        if (!writer)
            return;

        try
        {
            writer->finalize();
            writer->flush();
            write_buf->sync();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            release();
            throw;
        }
    }

    void release()
    {
        writer.reset();
        write_buf->finalize();
    }

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    std::mutex cancel_mutex;
    bool cancelled = false;
};

namespace
{
    std::optional<String> checkAndGetNewFileOnInsertIfNeeded(const ContextPtr & context, const String & uri, size_t sequence_number)
    {
        const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);

        HDFSBuilderWrapper builder = createHDFSBuilder(uri_without_path + "/", context->getGlobalContext()->getConfigRef());
        HDFSFSPtr fs = createHDFSFS(builder.get());

        if (context->getSettingsRef().hdfs_truncate_on_insert || hdfsExists(fs.get(), path_from_uri.c_str()))
            return std::nullopt;

        if (context->getSettingsRef().hdfs_create_new_file_on_insert)
        {
            auto pos = uri.find_first_of('.', uri.find_last_of('/'));
            String new_uri;
            do
            {
                new_uri = uri.substr(0, pos) + "." + std::to_string(sequence_number) + (pos == std::string::npos ? "" : uri.substr(pos));
                ++sequence_number;
            }
            while (!hdfsExists(fs.get(), new_uri.c_str()));

            return new_uri;
        }

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "File with path {} already exists. If you want to overwrite it, enable setting hdfs_truncate_on_insert, "
                "if you want to create new file on each insert, enable setting hdfs_create_new_file_on_insert",
                path_from_uri);
    }
}

class PartitionedHDFSSink : public PartitionedSink
{
public:
    PartitionedHDFSSink(
        const ASTPtr & partition_by,
        const String & uri_,
        const String & format_,
        const Block & sample_block_,
        ContextPtr context_,
        const CompressionMethod compression_method_)
            : PartitionedSink(partition_by, context_, sample_block_)
            , uri(uri_)
            , format(format_)
            , sample_block(sample_block_)
            , context(context_)
            , compression_method(compression_method_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto path = PartitionedSink::replaceWildcards(uri, partition_id);
        PartitionedSink::validatePartitionKey(path, true);
        if (auto new_path = checkAndGetNewFileOnInsertIfNeeded(context, path, 1))
            path = *new_path;
        return std::make_shared<HDFSSink>(path, format, sample_block, context, compression_method);
    }

private:
    const String uri;
    const String format;
    const Block sample_block;
    ContextPtr context;
    const CompressionMethod compression_method;
};


bool StorageHDFS::supportsSubsetOfColumns(const ContextPtr & context_) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context_);
}

class ReadFromHDFS : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromHDFS"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromHDFS(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        ReadFromFormatInfo info_,
        bool need_only_count_,
        std::shared_ptr<StorageHDFS> storage_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(
            DataStream{.header = std::move(sample_block)},
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , info(std::move(info_))
        , need_only_count(need_only_count_)
        , storage(std::move(storage_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

private:
    ReadFromFormatInfo info;
    const bool need_only_count;
    std::shared_ptr<StorageHDFS> storage;

    size_t max_block_size;
    size_t num_streams;

    std::shared_ptr<HDFSSource::IteratorWrapper> iterator_wrapper;

    void createIterator(const ActionsDAG::Node * predicate);
};

void ReadFromHDFS::applyFilters(ActionDAGNodes added_filter_nodes)
{
    filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createIterator(predicate);
}

void StorageHDFS::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(context_));
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && context_->getSettingsRef().optimize_count_from_files;

    auto this_ptr = std::static_pointer_cast<StorageHDFS>(shared_from_this());

    auto reading = std::make_unique<ReadFromHDFS>(
        column_names,
        query_info,
        storage_snapshot,
        context_,
        read_from_format_info.source_header,
        std::move(read_from_format_info),
        need_only_count,
        std::move(this_ptr),
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromHDFS::createIterator(const ActionsDAG::Node * predicate)
{
    if (iterator_wrapper)
        return;

    if (storage->distributed_processing)
    {
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>(
            [callback = context->getReadTaskCallback()]() -> StorageHDFS::PathWithInfo {
                return StorageHDFS::PathWithInfo{callback(), std::nullopt};
        });
    }
    else if (storage->is_path_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<HDFSSource::DisclosedGlobIterator>(storage->uris[0], predicate, storage->getVirtualsList(), context);
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>([glob_iterator]()
        {
            return glob_iterator->next();
        });
    }
    else
    {
        auto uris_iterator = std::make_shared<HDFSSource::URISIterator>(storage->uris, predicate, storage->getVirtualsList(), context);
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>([uris_iterator]()
        {
            return uris_iterator->next();
        });
    }
}

void ReadFromHDFS::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createIterator(nullptr);

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<HDFSSource>(
            info,
            storage,
            context,
            max_block_size,
            iterator_wrapper,
            need_only_count));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

SinkToStoragePtr StorageHDFS::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
{
    String current_uri = uris.front();

    bool has_wildcards = current_uri.find(PartitionedSink::PARTITION_ID_WILDCARD) != String::npos;
    const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
    bool is_partitioned_implementation = partition_by_ast && has_wildcards;

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedHDFSSink>(
            partition_by_ast,
            current_uri,
            format_name,
            metadata_snapshot->getSampleBlock(),
            context_,
            chooseCompressionMethod(current_uri, compression_method));
    }
    else
    {
        if (is_path_with_globs)
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "URI '{}' contains globs, so the table is in readonly mode", uris.back());

        if (auto new_uri = checkAndGetNewFileOnInsertIfNeeded(context_, uris.front(), uris.size()))
        {
            uris.push_back(*new_uri);
            current_uri = *new_uri;
        }

        return std::make_shared<HDFSSink>(current_uri,
            format_name,
            metadata_snapshot->getSampleBlock(),
            context_,
            chooseCompressionMethod(current_uri, compression_method));
    }
}

void StorageHDFS::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    const size_t begin_of_path = uris[0].find('/', uris[0].find("//") + 2);
    const String url = uris[0].substr(0, begin_of_path);

    HDFSBuilderWrapper builder = createHDFSBuilder(url + "/", local_context->getGlobalContext()->getConfigRef());
    auto fs = createHDFSFS(builder.get());

    for (const auto & uri : uris)
    {
        const String path = uri.substr(begin_of_path);
        int ret = hdfsDelete(fs.get(), path.data(), 0);
        if (ret)
            throw Exception(ErrorCodes::ACCESS_DENIED, "Unable to truncate hdfs table: {}", std::string(hdfsGetLastError()));
    }
}


void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage("HDFS", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.empty() || engine_args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage HDFS requires 1, 2 or 3 arguments: "
                            "url, name of used format (taken from file extension by default) and optional compression method.");

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.getLocalContext());

        String url = checkAndGetLiteralArgument<String>(engine_args[0], "url");

        String format_name = "auto";
        if (engine_args.size() > 1)
        {
            engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());
            format_name = checkAndGetLiteralArgument<String>(engine_args[1], "format_name");
        }

        if (format_name == "auto")
            format_name = FormatFactory::instance().tryGetFormatFromFileName(url).value_or("auto");

        String compression_method;
        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.getLocalContext());
            compression_method = checkAndGetLiteralArgument<String>(engine_args[2], "compression_method");
        } else compression_method = "auto";

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return std::make_shared<StorageHDFS>(
            url, args.table_id, format_name, args.columns, args.constraints, args.comment, args.getContext(), compression_method, false, partition_by);
    },
    {
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::HDFS,
    });
}

SchemaCache & StorageHDFS::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_hdfs", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

}

#endif
