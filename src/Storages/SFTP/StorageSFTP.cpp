#include <Common/parseGlobs.h>
#include <DataTypes/DataTypeString.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/Sources/ConstChunkGenerator.h>

#include <IO/WriteHelpers.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteSettings.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Storages/StorageFactory.h>
#include <Storages/SFTP/StorageSFTP.h>
#include <Storages/SFTP/ReadBufferFromSFTP.h>
#include <Storages/PartitionedSink.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Formats/ReadSchemaUtils.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionsConversion.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>

#include <Storages/SFTP/SSHWrapper.h>

#include <filesystem>

#ifdef __clang__
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif
#include <re2/re2.h>
#ifdef __clang__
#  pragma clang diagnostic pop
#endif

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
}
namespace
{

/// Forward-declare to use in expandSelector()
std::vector<StorageSFTP::PathWithInfo> LSWithRegexpMatching(const String & path_for_ls,
                                                            const std::shared_ptr<SFTPWrapper> &client,
                                                            const String & for_match);

/// Process {a,b,c...} globs separately: don't match it against regex, but generate a,b,c strings instead.
std::vector<StorageSFTP::PathWithInfo> expandSelector(const String & path_for_ls,
                                                        const std::shared_ptr<SFTPWrapper> &client,
                                                        const String & for_match)
{
    std::vector<size_t> anchor_positions = {};
    bool opened = false, closed = false;

    for (std::string::const_iterator it = for_match.begin(); it != for_match.end(); it++)
    {
        if (*it == '{')
        {
            anchor_positions.push_back(std::distance(for_match.begin(), it));
            opened = true;
        }
        else if (*it == '}')
        {
            anchor_positions.push_back(std::distance(for_match.begin(), it));
            closed = true;
            break;
        }
        else if (*it == ',')
        {
            if (!opened)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Unexpected ''' found in path '{}' at position {}.", for_match, std::distance(for_match.begin(), it));
            anchor_positions.push_back(std::distance(for_match.begin(), it));
        }
    }
    if (!opened || !closed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Invalid {{}} glob in path {}.", for_match);

    std::vector<StorageSFTP::PathWithInfo> ret = {};

    std::string common_prefix = for_match.substr(0, anchor_positions[0]);
    std::string common_suffix = for_match.substr(anchor_positions[anchor_positions.size()-1] + 1);
    for (size_t i = 1; i < anchor_positions.size(); ++i)
    {
        std::string expanded_matcher = common_prefix
                                        + for_match.substr(anchor_positions[i-1] + 1, (anchor_positions[i] - anchor_positions[i-1] - 1))
                                        + common_suffix;
        std::vector<StorageSFTP::PathWithInfo> result_part = LSWithRegexpMatching(path_for_ls, client, expanded_matcher);
        ret.insert(ret.end(), result_part.begin(), result_part.end());
    }
    return ret;
}

/* Recursive directory listing with matched paths as a result.
    * Have the same method in StorageFile.
    */
std::vector<StorageSFTP::PathWithInfo> LSWithRegexpMatching(
        const String & path_for_ls,
        const std::shared_ptr<SFTPWrapper> &client,
        const String & for_match)
{
    /// regexp for {expr1,expr2,expr3} or {M..N}, where M and N - non-negative integers, expr's should be without "{", "}", "*" and ","
    static const re2::RE2 enum_or_range(R"({([\d]+\.\.[\d]+|[^{}*,]+,[^{}*]*[^{}*,])})");

    std::string_view for_match_view(for_match);
    std::string_view matched;
    if (RE2::FindAndConsume(&for_match_view, enum_or_range, &matched))
    {
        std::string buffer(matched);
        if (buffer.find(',') != std::string::npos)
            return expandSelector(path_for_ls, client, for_match);
    }

    const size_t first_glob_pos = for_match.find_first_of("*?{");

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob_pos).rfind('/');
    const String suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'
    const String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'

    const size_t next_slash_after_glob_pos = suffix_with_globs.find('/', 1);

    const std::string current_glob = suffix_with_globs.substr(0, next_slash_after_glob_pos);

    re2::RE2 matcher(makeRegexpPatternFromGlobs(current_glob));
    if (!matcher.ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                        "Cannot compile regex from glob ({}): {}", for_match, matcher.error());

    SFTPWrapper::DirectoryIterator ls;

    try {
        ls = client->openDir(prefix_without_globs);
    } catch(...) {
        // TODO: access errors
        throw;
    }

    std::vector<StorageSFTP::PathWithInfo> result;
    for (SftpAttributes attrs = ls.next(); !ls.eof(); attrs = ls.next())
    {
        const String full_path = (fs::path(prefix_without_globs) / fs::path(attrs.getName())).lexically_normal();
        const size_t last_slash = full_path.rfind('/');
        const String file_name = full_path.substr(last_slash);
        const bool looking_for_directory = next_slash_after_glob_pos != std::string::npos;
        const bool is_directory = attrs.isDirectory();

        if (!is_directory && !looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher)) {
                result.push_back(StorageSFTP::PathWithInfo{
                        String(full_path),
                        StorageSFTP::PathInfo{static_cast<time_t>(attrs.getLastModifiedTime()),
                                                attrs.getSize()}});
            }
        }
        else if (is_directory && looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                std::vector<StorageSFTP::PathWithInfo> result_part = LSWithRegexpMatching(fs::path(full_path) / "", client,
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

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage SFTP requires valid URL to be set");
}

std::vector<StorageSFTP::PathWithInfo>
getPathsList(const String &path, const std::shared_ptr<SFTPWrapper> &client)
{
    auto res = LSWithRegexpMatching("/", client, path);
    return res;
}

}

StorageSFTP::StorageSFTP(
        Configuration configuration_,
        const StorageID & table_id_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const String & compression_method_,
        const bool distributed_processing_,
        ASTPtr partition_by_)
        : IStorage(table_id_)
        , WithContext(context_)
        , configuration(configuration_)
        , format_name(format_name_)
        , compression_method(compression_method_)
        , distributed_processing(distributed_processing_)
        , partition_by(partition_by_)
{
    std::shared_ptr<SSHWrapper> ssh_wrapper;
    if (configuration.port == 0) {
        configuration.port = 22;
    }
    if (!configuration.password.empty()) {
        ssh_wrapper = std::make_shared<SSHWrapper>(configuration.user, configuration.password, configuration.host,
                                                    configuration.port);
    } else {
        ssh_wrapper = std::make_shared<SSHWrapper>(configuration.user, configuration.host, configuration.port);
    }

    client = std::make_shared<SFTPWrapper>(ssh_wrapper);

    if (!configuration.path.starts_with('/')) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path in SFTP storage must start with '/'");
    }
    uri = "sftp://" + configuration.user + "@" + configuration.host + ":" + std::to_string(configuration.port) + configuration.path;
    FormatFactory::instance().checkFormatName(format_name);

    is_path_with_globs = configuration.path.find_first_of("*?{") != std::string::npos;

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(format_name, client, uri, configuration.path, compression_method, context_);
        storage_metadata.setColumns(columns);
    }
    else
    {
        /// We don't allow special columns in SFTP storage.
        if (!columns_.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table engine SFTP doesn't support special columns like MATERIALIZED, ALIAS or EPHEMERAL");
        storage_metadata.setColumns(columns_);
    }
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
}

namespace
{
    class ReadBufferIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferIterator(
                const std::vector<StorageSFTP::PathWithInfo> & paths_with_info_,
                const std::shared_ptr<SFTPWrapper> &client_,
                const String & format_,
                const String & compression_method_,
                const ContextPtr & context_)
                : WithContext(context_)
                , paths_with_info(paths_with_info_)
                , client(client_)
                , format(format_)
                , compression_method(compression_method_)
        {
        }

        Data next() override
        {
            StorageSFTP::PathWithInfo path_with_info;
            bool is_first = current_index == 0;

            while (true)
            {
                if (current_index == paths_with_info.size())
                {
                    if (is_first)
                        throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                                        "Cannot extract table structure from {} format file, because all files are empty. "
                                        "You must specify table structure manually", format);
                    return Data{.buf = nullptr};
                }

                path_with_info = paths_with_info[current_index++];
                if (getContext()->getSettingsRef().sftp_skip_empty_files && path_with_info.info && path_with_info.info->size == 0)
                    continue;

                auto compression = chooseCompressionMethod(path_with_info.path, compression_method);
                auto impl = std::make_unique<ReadBufferFromSFTP>(client, path_with_info.path, getContext()->getReadSettings());
                if (!getContext()->getSettingsRef().sftp_skip_empty_files || !impl->eof())
                {
                    const Int64 zstd_window_log_max = getContext()->getSettingsRef().zstd_window_log_max;
                    return Data{.buf = wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max))};
                }
            }
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_sftp)
                return;

            String source = paths_with_info[current_index - 1].path;
            auto key = getKeyForSchemaCache(source, format, std::nullopt, getContext());
            StorageSFTP::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

    private:
        const std::vector<StorageSFTP::PathWithInfo> & paths_with_info;
        std::shared_ptr<SFTPWrapper> client;
        const String & format;
        const String & compression_method;
        size_t current_index = 0;
    };
}

ColumnsDescription StorageSFTP::getTableStructureFromData(
        const String & format,
        const std::shared_ptr<SFTPWrapper> &client,
        const String & uri,
        const String & path,
        const String & compression_method,
        ContextPtr ctx)
{
    auto paths_with_info = getPathsList(path, client);

    if (paths_with_info.empty() && !FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format))
        throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Cannot extract table structure from {} format file, because there are no files in SFTP with provided path."
                " You must specify table structure manually", format);

    std::optional<ColumnsDescription> columns_from_cache;
    if (ctx->getSettingsRef().schema_inference_use_cache_for_sftp)
        columns_from_cache = tryGetColumnsFromCache(client, paths_with_info, uri, format, ctx);


    ColumnsDescription columns;
    if (columns_from_cache)
    {
        columns = *columns_from_cache;
    }
    else
    {
        ReadBufferIterator read_buffer_iterator(paths_with_info, client, format, compression_method, ctx);
        columns = readSchemaFromFormat(format, std::nullopt, read_buffer_iterator, ctx);
    }

    if (ctx->getSettingsRef().schema_inference_use_cache_for_sftp)
        addColumnsToCache(paths_with_info, uri, columns, format, ctx);

    return columns;
}

class SFTPSource::DisclosedGlobIterator::Impl
{
public:
    Impl(
        const std::shared_ptr<SFTPWrapper> &client_,
        const String &path,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList &virtual_columns,
        const ContextPtr &context
    )
    {
        uris = getPathsList(path, client_);
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
            if (file_progress_callback && elem.info)
                file_progress_callback(FileProgress(0, elem.info->size));
        }
        uris_iter = uris.begin();
    }

    StorageSFTP::PathWithInfo next()
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
    std::vector<StorageSFTP::PathWithInfo> uris;
    std::vector<StorageSFTP::PathWithInfo>::iterator uris_iter;
};

class SFTPSource::URISIterator::Impl : WithContext
{
public:
    Impl(
        const std::shared_ptr<SFTPWrapper> &client_,
        const std::vector<String> &uris_with_paths_,
        const ActionsDAG::Node * predicate,
        const NamesAndTypesList &virtual_columns,
        const ContextPtr &context_
    )
        : WithContext(context_)
        , uris(uris_with_paths_)
        , client(client_)
        , file_progress_callback(context_->getFileProgressCallback())
    {
        ActionsDAGPtr filter_fag;
        if (!uris.empty())
            filter_fag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);

        if (filter_fag)
        {
            std::vector<String> paths;
            paths.reserve(uris.size());
            for (const auto & uri : uris)
                paths.push_back(getPathFromUriAndUriWithoutPath(uri).first);

            VirtualColumnUtils::filterByPathOrFile(uris, paths, filter_fag, virtual_columns, getContext());
        }
    }

    StorageSFTP::PathWithInfo next()
    {
        String uri;
        SftpAttributes sftp_info;
        do
        {
            size_t current_index = index.fetch_add(1);
            if (current_index >= uris.size())
                return {"", {}};

            uri = uris[current_index];
            auto path_and_uri = getPathFromUriAndUriWithoutPath(uri);
            try {
                sftp_info = client->getPathInfo(path_and_uri.first);
            } catch(...) {
                // skip non-existent files
                continue;
            }
        }
        while (!sftp_info);

        std::optional<StorageSFTP::PathInfo> info;
        if (sftp_info)
        {
            info = StorageSFTP::PathInfo{static_cast<time_t>(sftp_info.getLastModifiedTime()),sftp_info.getSize()};
            if (file_progress_callback)
                file_progress_callback(FileProgress(0, sftp_info.getSize()));
        }

        return {getPathFromUriAndUriWithoutPath(uri).first, info};
    }

private:
    std::atomic_size_t index = 0;
    Strings uris;
    std::shared_ptr<SFTPWrapper> client;
    std::function<void(FileProgress)> file_progress_callback;
};

SFTPSource::DisclosedGlobIterator::DisclosedGlobIterator(
    const std::shared_ptr<SFTPWrapper> &client_,
    const String &path,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList &virtual_columns,
    const ContextPtr &context)
    : pimpl(std::make_shared<SFTPSource::DisclosedGlobIterator::Impl>(client_, path, predicate, virtual_columns, context))
{}

StorageSFTP::PathWithInfo SFTPSource::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

SFTPSource::URISIterator::URISIterator(
    const std::shared_ptr<SFTPWrapper> &client_,
    const std::vector<String> &uris_with_paths_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList &virtual_columns,
    const ContextPtr &context)
    : pimpl(std::make_shared<SFTPSource::URISIterator::Impl>(client_, uris_with_paths_, predicate, virtual_columns, context))
{}

StorageSFTP::PathWithInfo SFTPSource::URISIterator::next()
{
    return pimpl->next();
}

SFTPSource::SFTPSource(
    const StorageSFTP::Configuration &configuration_,
    const ReadFromFormatInfo & info,
    StorageSFTPPtr storage_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IteratorWrapper> file_iterator_,
    bool need_only_count_,
    const SelectQueryInfo & query_info_)
    : ISource(info.source_header, false)
    , WithContext(context_)
    , configuration(configuration_)
    , storage(std::move(storage_))
    , block_for_format(info.format_header)
    , requested_columns(info.requested_columns)
    , requested_virtual_columns(info.requested_virtual_columns)
    , max_block_size(max_block_size_)
    , file_iterator(file_iterator_)
    , columns_description(info.columns_description)
    , need_only_count(need_only_count_)
    , query_info(query_info_)
{
    initialize();
}

bool SFTPSource::initialize()
{
    std::shared_ptr<SSHWrapper> ssh_wrapper;
    if (!configuration.password.empty()) {
        ssh_wrapper = std::make_shared<SSHWrapper>(configuration.user, configuration.password, configuration.host,
                                                    configuration.port);
    } else {
        ssh_wrapper = std::make_shared<SSHWrapper>(configuration.user, configuration.host, configuration.port);
    }

    client = std::make_shared<SFTPWrapper>(ssh_wrapper);

    bool skip_empty_files = getContext()->getSettingsRef().sftp_skip_empty_files;
    StorageSFTP::PathWithInfo path_with_info;
    while (true)
    {
        path_with_info = (*file_iterator)();
        if (path_with_info.path.empty())
            return false;

        if (path_with_info.info && skip_empty_files && path_with_info.info->size == 0)
            continue;

        current_path = path_with_info.path;

        std::optional<size_t> file_size;
        if (!path_with_info.info)
        {
            auto sftp_info = client->getPathInfo(current_path);
            if (sftp_info)
                path_with_info.info = StorageSFTP::PathInfo{static_cast<time_t>(sftp_info.getLastModifiedTime()), sftp_info.getSize()};
        }

        if (path_with_info.info)
            file_size = path_with_info.info->size;

        auto compression = chooseCompressionMethod(current_path, storage->compression_method);
        auto impl = std::make_unique<ReadBufferFromSFTP>(
                client, current_path, getContext()->getReadSettings(), 0, false);
        if (!skip_empty_files || !impl->eof())
        {
            impl->setProgressCallback(getContext());
            const Int64 zstd_window_log_max = getContext()->getSettingsRef().zstd_window_log_max;
            read_buf = wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max));
            break;
        }
    }

    current_path = path_with_info.path;

    QueryPipelineBuilder builder;
    std::optional<size_t> num_rows_from_cache = need_only_count && getContext()->getSettingsRef().use_cache_for_count_from_files ? tryGetNumRowsFromCache(path_with_info) : std::nullopt;
    if (num_rows_from_cache)
    {
        /// We should not return single chunk with all number of rows,
        /// because there is a chance that this chunk will be materialized later
        /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
        /// Instead, we use special ConstChunkGenerator that will generate chunks
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
        // TODO: What is it?
        // input_format->setQueryInfo(query_info, getContext());

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
    /// from chunk read by IInputFormat.
    builder.addSimpleTransform([&](const Block & header)
                                {
                                    return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
                                });

    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);
    return true;
}

String SFTPSource::getName() const
{
    return "SFTPSource";
}

Chunk SFTPSource::generate()
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
            /// TODO: Figure out what it does what to pass here.
            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(chunk, requested_virtual_columns, current_path, std::nullopt);
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

void SFTPSource::addNumRowsToCache(const String & path, size_t num_rows)
{
    auto cache_key = getKeyForSchemaCache(path, storage->format_name, std::nullopt, getContext());
    StorageSFTP::getSchemaCache(getContext()).addNumRows(cache_key, num_rows);
}

std::optional<size_t> SFTPSource::tryGetNumRowsFromCache(const StorageSFTP::PathWithInfo & path_with_info)
{
    auto cache_key = getKeyForSchemaCache(path_with_info.path, storage->format_name, std::nullopt, getContext());
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        if (path_with_info.info)
            return path_with_info.info->last_mod_time;
        return std::nullopt;
    };

    return StorageSFTP::getSchemaCache(getContext()).tryGetNumRows(cache_key, get_last_mod_time);
}

//    class SFTPSink : public SinkToStorage
//    {
//    public:
//        SFTPSink(const String & uri,
//                 const String & format,
//                 const Block & sample_block,
//                 ContextPtr context,
//                 const CompressionMethod compression_method)
//                : SinkToStorage(sample_block)
//        {
//            write_buf = wrapWriteBufferWithCompressionMethod(
//                    std::make_unique<WriteBufferFromSFTP>(
//                            uri,
//                            context->getGlobalContext()->getConfigRef(),
//                            context->getSettingsRef().SFTP_replication,
//                            context->getWriteSettings()),
//                    compression_method, 3);
//            writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context);
//        }
//
//        String getName() const override { return "SFTPSink"; }
//
//        void consume(Chunk chunk) override
//        {
//            std::lock_guard lock(cancel_mutex);
//            if (cancelled)
//                return;
//            writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
//        }
//
//        void onCancel() override
//        {
//            std::lock_guard lock(cancel_mutex);
//            finalize();
//            cancelled = true;
//        }
//
//        void onException(std::exception_ptr exception) override
//        {
//            std::lock_guard lock(cancel_mutex);
//            try
//            {
//                std::rethrow_exception(exception);
//            }
//            catch (...)
//            {
//                /// An exception context is needed to proper delete write buffers without finalization
//                release();
//            }
//        }
//
//        void onFinish() override
//        {
//            std::lock_guard lock(cancel_mutex);
//            finalize();
//        }
//
//    private:
//        void finalize()
//        {
//            if (!writer)
//                return;
//
//            try
//            {
//                writer->finalize();
//                writer->flush();
//                write_buf->sync();
//                write_buf->finalize();
//            }
//            catch (...)
//            {
//                /// Stop ParallelFormattingOutputFormat correctly.
//                release();
//                throw;
//            }
//        }
//
//        void release()
//        {
//            writer.reset();
//            write_buf->finalize();
//        }
//
//        std::unique_ptr<WriteBuffer> write_buf;
//        OutputFormatPtr writer;
//        std::mutex cancel_mutex;
//        bool cancelled = false;
//    };

//    class PartitionedSFTPSink : public PartitionedSink
//    {
//    public:
//        PartitionedSFTPSink(
//                const ASTPtr & partition_by,
//                const String & uri_,
//                const String & format_,
//                const Block & sample_block_,
//                ContextPtr context_,
//                const CompressionMethod compression_method_)
//                : PartitionedSink(partition_by, context_, sample_block_)
//                , uri(uri_)
//                , format(format_)
//                , sample_block(sample_block_)
//                , context(context_)
//                , compression_method(compression_method_)
//        {
//        }
//
//        SinkPtr createSinkForPartition(const String & partition_id) override
//        {
//            auto path = PartitionedSink::replaceWildcards(uri, partition_id);
//            PartitionedSink::validatePartitionKey(path, true);
//            return std::make_shared<SFTPSink>(path, format, sample_block, context, compression_method);
//        }
//
//    private:
//        const String uri;
//        const String format;
//        const Block sample_block;
//        ContextPtr context;
//        const CompressionMethod compression_method;
//    };


bool StorageSFTP::supportsSubsetOfColumns(const ContextPtr & context_) const
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context_);
}

Pipe StorageSFTP::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams)
{
    std::shared_ptr<SFTPSource::IteratorWrapper> iterator_wrapper{nullptr};
    if (distributed_processing)
    {
        iterator_wrapper = std::make_shared<SFTPSource::IteratorWrapper>(
                [callback = context_->getReadTaskCallback()]() -> StorageSFTP::PathWithInfo {
                    return StorageSFTP::PathWithInfo{callback(), std::nullopt};
                });
    }
    else if (is_path_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<SFTPSource::DisclosedGlobIterator>(client, configuration.path, query_info.query, virtual_columns, context_);
        iterator_wrapper = std::make_shared<SFTPSource::IteratorWrapper>([glob_iterator]()
                                                                            {
                                                                                return glob_iterator->next();
                                                                            });
    }
    else
    {
        auto uris_iterator = std::make_shared<SFTPSource::URISIterator>(client, std::vector{uri}, query_info.query, virtual_columns, context_);
        iterator_wrapper = std::make_shared<SFTPSource::IteratorWrapper>([uris_iterator]()
                                                                            {
                                                                                return uris_iterator->next();
                                                                            });
    }

    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(context_), getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
                            && context_->getSettingsRef().optimize_count_from_files;

    Pipes pipes;
    auto this_ptr = std::static_pointer_cast<StorageSFTP>(shared_from_this());
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<SFTPSource>(
                configuration,
                read_from_format_info,
                this_ptr,
                context_,
                max_block_size,
                iterator_wrapper,
                need_only_count,
                query_info));
    }
    return Pipe::unitePipes(std::move(pipes));
}

//    SinkToStoragePtr StorageSFTP::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
//    {
//        String current_uri = uris.back();
//
//        bool has_wildcards = current_uri.find(PartitionedSink::PARTITION_ID_WILDCARD) != String::npos;
//        const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
//        auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : partition_by) : nullptr;
//        bool is_partitioned_implementation = partition_by_ast && has_wildcards;
//
//        if (is_partitioned_implementation)
//        {
//            return std::make_shared<PartitionedSFTPSink>(
//                    partition_by_ast,
//                    current_uri,
//                    format_name,
//                    metadata_snapshot->getSampleBlock(),
//                    context_,
//                    chooseCompressionMethod(current_uri, compression_method));
//        }
//        else
//        {
//            if (is_path_with_globs)
//                throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "URI '{}' contains globs, so the table is in readonly mode", uris.back());
//
//            const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(current_uri);
//
//            SFTPBuilderWrapper builder = createSFTPBuilder(uri_without_path + "/", context_->getGlobalContext()->getConfigRef());
//            SFTPFSPtr fs = createSFTPFS(builder.get());
//
//            bool truncate_on_insert = context_->getSettingsRef().SFTP_truncate_on_insert;
//            if (!truncate_on_insert && !SFTPExists(fs.get(), path_from_uri.c_str()))
//            {
//                if (context_->getSettingsRef().SFTP_create_new_file_on_insert)
//                {
//                    auto pos = uris[0].find_first_of('.', uris[0].find_last_of('/'));
//                    size_t index = uris.size();
//                    String new_uri;
//                    do
//                    {
//                        new_uri = uris[0].substr(0, pos) + "." + std::to_string(index) + (pos == std::string::npos ? "" : uris[0].substr(pos));
//                        ++index;
//                    }
//                    while (!SFTPExists(fs.get(), new_uri.c_str()));
//                    uris.push_back(new_uri);
//                    current_uri = new_uri;
//                }
//                else
//                    throw Exception(
//                            ErrorCodes::BAD_ARGUMENTS,
//                            "File with path {} already exists. If you want to overwrite it, enable setting SFTP_truncate_on_insert, "
//                            "if you want to create new file on each insert, enable setting SFTP_create_new_file_on_insert",
//                            path_from_uri);
//            }
//
//            return std::make_shared<SFTPSink>(current_uri,
//                                              format_name,
//                                              metadata_snapshot->getSampleBlock(),
//                                              context_,
//                                              chooseCompressionMethod(current_uri, compression_method));
//        }
//    }

//    void StorageSFTP::truncate(const ASTPtr & /* query */, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
//    {
//        const size_t begin_of_path = uris[0].find('/', uris[0].find("//") + 2);
//        const String url = uris[0].substr(0, begin_of_path);
//
//        SFTPBuilderWrapper builder = createSFTPBuilder(url + "/", local_context->getGlobalContext()->getConfigRef());
//        auto fs = createSFTPFS(builder.get());
//
//        for (const auto & uri : uris)
//        {
//            const String path = uri.substr(begin_of_path);
//            int ret = SFTPDelete(fs.get(), path.data(), 0);
//            if (ret)
//                throw Exception(ErrorCodes::ACCESS_DENIED, "Unable to truncate SFTP table: {}", std::string(SFTPGetLastError()));
//        }
//    }


void registerStorageSFTP(StorageFactory & factory)
{
    factory.registerStorage("SFTP", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.empty() || engine_args.size() > 8)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage sftp requires 4, 5, 6 or 7 arguments: "
                            "host, path, user, password | DAEMON_AUTH, port (22 by default), name of used format (taken from file extension by default) and optional compression method.");

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.getLocalContext());

        StorageSFTP::Configuration configuration;

        configuration.host = checkAndGetLiteralArgument<String>(engine_args[0], "host");
        configuration.path = checkAndGetLiteralArgument<String>(engine_args[1], "path");
        configuration.user = checkAndGetLiteralArgument<String>(engine_args[2], "user");

        String password = checkAndGetLiteralArgument<String>(engine_args[3], "password");

        if (password == "DAEMON_AUTH")
        {
            configuration.password = "";
        }
        else
        {
            configuration.password = password;
        }

        UInt16 port = 22;

        size_t curr_arg = 4;

        if (engine_args.size() > curr_arg) {
            try {
                port = checkAndGetLiteralArgument<UInt64>(engine_args[curr_arg], "port");
                ++curr_arg;
            }
            catch (...) {

            }
        }
        configuration.port = port;

        String format_name = "auto";
        if (engine_args.size() > curr_arg)
        {
            engine_args[curr_arg] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[curr_arg], args.getLocalContext());
            format_name = checkAndGetLiteralArgument<String>(engine_args[curr_arg], "format_name");
            ++curr_arg;
        }

        if (format_name == "auto")
            format_name = FormatFactory::instance().getFormatFromFileName(configuration.path);

        String compression_method;
        if (engine_args.size() > curr_arg)
        {
            engine_args[curr_arg] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[curr_arg], args.getLocalContext());
            compression_method = checkAndGetLiteralArgument<String>(engine_args[curr_arg], "compression_method");
        } else compression_method = "auto";

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return std::make_shared<StorageSFTP>(
                configuration, args.table_id, format_name, args.columns, args.constraints, args.comment, args.getContext(), compression_method, false, partition_by);
    },
    {
            .supports_sort_order = true, // for partition by
            .supports_schema_inference = true,
            .source_access_type = AccessType::SFTP,
    });
}

NamesAndTypesList StorageSFTP::getVirtuals() const
{
    return virtual_columns;
}

SchemaCache & StorageSFTP::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_SFTP", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<ColumnsDescription> StorageSFTP::tryGetColumnsFromCache(
        const std::shared_ptr<SFTPWrapper> &client,
        const std::vector<StorageSFTP::PathWithInfo> & paths_with_info,
        const String & uri_without_path,
        const String & format_name,
        const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    for (const auto & path_with_info : paths_with_info)
    {
        auto get_last_mod_time = [&, client]() -> std::optional<time_t>
        {
            if (path_with_info.info)
                return path_with_info.info->last_mod_time;
            auto sftp_attributes = client->getPathInfo(path_with_info.path);
            return sftp_attributes ? std::make_optional(sftp_attributes.getLastModifiedTime()) : std::nullopt;
        };
        String url = uri_without_path + path_with_info.path;
        auto cache_key = getKeyForSchemaCache(url, format_name, {}, ctx);
        auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void StorageSFTP::addColumnsToCache(
        const std::vector<StorageSFTP::PathWithInfo> & paths_with_info,
        const String & uri_without_path,
        const ColumnsDescription & columns,
        const String & format_name,
        const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    Strings sources;
    sources.reserve(paths_with_info.size());
    std::transform(paths_with_info.begin(), paths_with_info.end(), std::back_inserter(sources), [&](const PathWithInfo & path_with_info){ return uri_without_path + path_with_info.path; });
    auto cache_keys = getKeysForSchemaCache(sources, format_name, {}, ctx);
    schema_cache.addManyColumns(cache_keys, columns);
}

}
