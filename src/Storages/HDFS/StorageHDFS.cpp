#include "config.h"

#if USE_HDFS

#include <Common/parseGlobs.h>
#include <DataTypes/DataTypeString.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>

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
#include <Storages/getVirtualsForStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/ReadFromStorageProgress.h>

#include <Formats/ReadSchemaUtils.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionsConversion.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>

#include <Poco/URI.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <hdfs/hdfs.h>

#include <filesystem>


namespace fs = std::filesystem;

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
    /// Forward-declared to use in LSWithFoldedRegexpMatching w/o circular dependency.
    std::vector<StorageHDFS::PathWithInfo> LSWithRegexpMatching(const String & path_for_ls,
                                                                const HDFSFSPtr & fs,
                                                                const String & for_match);

    /*
     * When `{...}` has any `/`s, it must be processed in a different way:
     * Basically, a path with globs is processed by LSWithRegexpMatching. In case it detects multi-dir glob {.../..., .../...},
     * LSWithFoldedRegexpMatching is in charge from now on.
     * It works a bit different: it still recursively goes through subdirectories, but does not match every directory to glob.
     * Instead, it goes many levels down (until the approximate max_depth is reached) and compares this multi-dir path to a glob.
     * StorageFile.cpp has the same logic.
    */
    std::vector<StorageHDFS::PathWithInfo> LSWithFoldedRegexpMatching(const String & path_for_ls,
        const HDFSFSPtr & fs,
        const String & processed_suffix,
        const String & suffix_with_globs,
        re2::RE2 & matcher,
        const size_t max_depth,
        const size_t next_slash_after_glob_pos)
    {
        /// We don't need to go all the way in every directory if max_depth is reached
        /// as it is upper limit of depth by simply counting `/`s in curly braces
        if (!max_depth)
            return {};

        HDFSFileInfo ls;
        ls.file_info = hdfsListDirectory(fs.get(), path_for_ls.data(), &ls.length);
        if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
        {
            // ignore file not found exception, keep throw other exception, libhdfs3 doesn't have function to get exception type, so use errno.
            throw Exception(
                ErrorCodes::ACCESS_DENIED, "Cannot list directory {}: {}", path_for_ls, String(hdfsGetLastError()));
        }

        std::vector<StorageHDFS::PathWithInfo> result;

        if (!ls.file_info && ls.length > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "file_info shouldn't be null");

        for (int i = 0; i < ls.length; ++i)
        {
            const String full_path = String(ls.file_info[i].mName);
            const size_t last_slash = full_path.rfind('/');
            const String dir_or_file_name = full_path.substr(last_slash);
            const bool is_directory = ls.file_info[i].mKind == 'D';

            if (re2::RE2::FullMatch(processed_suffix + dir_or_file_name, matcher))
            {
                if (next_slash_after_glob_pos == std::string::npos)
                {
                    result.emplace_back(
                        String(ls.file_info[i].mName),
                        StorageHDFS::PathInfo{ls.file_info[i].mLastMod, static_cast<size_t>(ls.file_info[i].mSize)});
                }
                else
                {
                    std::vector<StorageHDFS::PathWithInfo> result_part = LSWithRegexpMatching(
                        fs::path(full_path) / "" , fs, suffix_with_globs.substr(next_slash_after_glob_pos));
                    std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
                }
            }
            else if (is_directory)
            {
                std::vector<StorageHDFS::PathWithInfo> result_part = LSWithFoldedRegexpMatching(
                    fs::path(full_path), fs, processed_suffix + dir_or_file_name,
                    suffix_with_globs, matcher, max_depth - 1, next_slash_after_glob_pos);
                std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
            }
        }
        return result;
    }

    /* Recursive directory listing with matched paths as a result.
     * Have the same method in StorageFile.
     */
    std::vector<StorageHDFS::PathWithInfo> LSWithRegexpMatching(
        const String & path_for_ls,
        const HDFSFSPtr & fs,
        const String & for_match)
    {
        const size_t first_glob_pos = for_match.find_first_of("*?{");
        const bool has_glob = first_glob_pos != std::string::npos;

        const size_t end_of_path_without_globs = for_match.substr(0, first_glob_pos).rfind('/');
        const String suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'
        const String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'

        size_t slashes_in_glob = 0;
        const size_t next_slash_after_glob_pos = [&]()
        {
            if (!has_glob)
                return suffix_with_globs.find('/', 1);

            size_t in_curly = 0;
            for (std::string::const_iterator it = ++suffix_with_globs.begin(); it != suffix_with_globs.end(); it++)
            {
                if (*it == '{')
                    ++in_curly;
                else if (*it == '/')
                {
                    if (in_curly)
                        ++slashes_in_glob;
                    else
                        return size_t(std::distance(suffix_with_globs.begin(), it));
                }
                else if (*it == '}')
                    --in_curly;
            }
            return std::string::npos;
        }();

        const std::string current_glob = suffix_with_globs.substr(0, next_slash_after_glob_pos);

        re2::RE2 matcher(makeRegexpPatternFromGlobs(current_glob));
        if (!matcher.ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regex from glob ({}): {}", for_match, matcher.error());

        if (slashes_in_glob)
        {
            return LSWithFoldedRegexpMatching(fs::path(prefix_without_globs), fs, "", suffix_with_globs,
                                              matcher, slashes_in_glob, next_slash_after_glob_pos);
        }

        HDFSFileInfo ls;
        ls.file_info = hdfsListDirectory(fs.get(), prefix_without_globs.data(), &ls.length);
        if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
        {
            // ignore file not found exception, keep throw other exception, libhdfs3 doesn't have function to get exception type, so use errno.
            throw Exception(
                ErrorCodes::ACCESS_DENIED, "Cannot list directory {}: {}", prefix_without_globs, String(hdfsGetLastError()));
        }
        std::vector<StorageHDFS::PathWithInfo> result;
        if (!ls.file_info && ls.length > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "file_info shouldn't be null");
        for (int i = 0; i < ls.length; ++i)
        {
            const String full_path = String(ls.file_info[i].mName);
            const size_t last_slash = full_path.rfind('/');
            const String file_name = full_path.substr(last_slash);
            const bool looking_for_directory = next_slash_after_glob_pos != std::string::npos;
            const bool is_directory = ls.file_info[i].mKind == 'D';
            /// Condition with type of current file_info means what kind of path is it in current iteration of ls
            if (!is_directory && !looking_for_directory)
            {
                if (re2::RE2::FullMatch(file_name, matcher))
                    result.push_back(StorageHDFS::PathWithInfo{
                        String(ls.file_info[i].mName),
                        StorageHDFS::PathInfo{ls.file_info[i].mLastMod, static_cast<size_t>(ls.file_info[i].mSize)}});
            }
            else if (is_directory && looking_for_directory)
            {
                if (re2::RE2::FullMatch(file_name, matcher))
                {
                    std::vector<StorageHDFS::PathWithInfo> result_part = LSWithRegexpMatching(fs::path(full_path) / "", fs, suffix_with_globs.substr(next_slash_after_glob_pos));
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

        return LSWithRegexpMatching("/", fs, path_from_uri);
    }
}

StorageHDFS::StorageHDFS(
    const String & uri_,
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
    , uris({uri_})
    , format_name(format_name_)
    , compression_method(compression_method_)
    , distributed_processing(distributed_processing_)
    , partition_by(partition_by_)
{
    FormatFactory::instance().checkFormatName(format_name);
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri_));
    checkHDFSURL(uri_);

    String path = uri_.substr(uri_.find('/', uri_.find("//") + 2));
    is_path_with_globs = path.find_first_of("*?{") != std::string::npos;

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(format_name, uri_, compression_method, context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
}

ColumnsDescription StorageHDFS::getTableStructureFromData(
    const String & format,
    const String & uri,
    const String & compression_method,
    ContextPtr ctx)
{
    const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);
    auto paths_with_info = getPathsList(path_from_uri, uri, ctx);
    if (paths_with_info.empty() && !FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format))
        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "Cannot extract table structure from {} format file, because there are no files in HDFS with provided path."
            " You must specify table structure manually", format);

    std::optional<ColumnsDescription> columns_from_cache;
    if (ctx->getSettingsRef().schema_inference_use_cache_for_hdfs)
        columns_from_cache = tryGetColumnsFromCache(paths_with_info, path_from_uri, format, ctx);

    ReadBufferIterator read_buffer_iterator
        = [&, my_uri_without_path = uri_without_path, it = paths_with_info.begin(), first = true](
              ColumnsDescription &) mutable -> std::unique_ptr<ReadBuffer>
    {
        PathWithInfo path_with_info;
        while (true)
        {
            if (it == paths_with_info.end())
            {
                if (first)
                    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                                    "Cannot extract table structure from {} format file, because all files are empty. "
                                    "You must specify table structure manually", format);
                return nullptr;
            }

            path_with_info = *it++;
            if (ctx->getSettingsRef().hdfs_skip_empty_files && path_with_info.info && path_with_info.info->size == 0)
                continue;

            auto compression = chooseCompressionMethod(path_with_info.path, compression_method);
            auto impl = std::make_unique<ReadBufferFromHDFS>(my_uri_without_path, path_with_info.path, ctx->getGlobalContext()->getConfigRef(), ctx->getReadSettings());
            if (!ctx->getSettingsRef().hdfs_skip_empty_files || !impl->eof())
            {
                const Int64 zstd_window_log_max = ctx->getSettingsRef().zstd_window_log_max;
                first = false;
                return wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max));
            }
        }
    };

    ColumnsDescription columns;
    if (columns_from_cache)
        columns = *columns_from_cache;
    else
        columns = readSchemaFromFormat(format, std::nullopt, read_buffer_iterator, paths_with_info.size() > 1, ctx);

    if (ctx->getSettingsRef().schema_inference_use_cache_for_hdfs)
        addColumnsToCache(paths_with_info, path_from_uri, columns, format, ctx);

    return columns;
}

class HDFSSource::DisclosedGlobIterator::Impl
{
public:
    Impl(ContextPtr context_, const String & uri)
    {
        const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);
        uris = getPathsList(path_from_uri, uri_without_path, context_);
        for (auto & elem : uris)
            elem.path = uri_without_path + elem.path;
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

class HDFSSource::URISIterator::Impl
{
public:
    explicit Impl(const std::vector<String> & uris_, ContextPtr context)
    {
        auto path_and_uri = getPathFromUriAndUriWithoutPath(uris_[0]);
        HDFSBuilderWrapper builder = createHDFSBuilder(path_and_uri.second + "/", context->getGlobalContext()->getConfigRef());
        auto fs = createHDFSFS(builder.get());
        for (const auto & uri : uris_)
        {
            path_and_uri = getPathFromUriAndUriWithoutPath(uri);
            if (!hdfsExists(fs.get(), path_and_uri.first.c_str()))
                uris.push_back(uri);
        }
        uris_iter = uris.begin();
    }

    StorageHDFS::PathWithInfo next()
    {
        std::lock_guard lock(mutex);
        if (uris_iter == uris.end())
            return {"", {}};
        auto key = *uris_iter;
        ++uris_iter;
        return {key, {}};
    }

private:
    std::mutex mutex;
    Strings uris;
    Strings::iterator uris_iter;
};

HDFSSource::DisclosedGlobIterator::DisclosedGlobIterator(ContextPtr context_, const String & uri)
    : pimpl(std::make_shared<HDFSSource::DisclosedGlobIterator::Impl>(context_, uri)) {}

StorageHDFS::PathWithInfo HDFSSource::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

HDFSSource::URISIterator::URISIterator(const std::vector<String> & uris_, ContextPtr context)
    : pimpl(std::make_shared<HDFSSource::URISIterator::Impl>(uris_, context))
{
}

StorageHDFS::PathWithInfo HDFSSource::URISIterator::next()
{
    return pimpl->next();
}

Block HDFSSource::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}

HDFSSource::HDFSSource(
    StorageHDFSPtr storage_,
    const Block & block_for_format_,
    const std::vector<NameAndTypePair> & requested_virtual_columns_,
    ContextPtr context_,
    UInt64 max_block_size_,
    std::shared_ptr<IteratorWrapper> file_iterator_,
    ColumnsDescription columns_description_)
    : ISource(getHeader(block_for_format_, requested_virtual_columns_))
    , WithContext(context_)
    , storage(std::move(storage_))
    , block_for_format(block_for_format_)
    , requested_virtual_columns(requested_virtual_columns_)
    , max_block_size(max_block_size_)
    , file_iterator(file_iterator_)
    , columns_description(std::move(columns_description_))
{
    initialize();
}

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

        auto compression = chooseCompressionMethod(path_from_uri, storage->compression_method);
        auto impl = std::make_unique<ReadBufferFromHDFS>(
            uri_without_path, path_from_uri, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
        if (!skip_empty_files || !impl->eof())
        {
            const Int64 zstd_window_log_max = getContext()->getSettingsRef().zstd_window_log_max;
            read_buf = wrapReadBufferWithCompressionMethod(std::move(impl), compression, static_cast<int>(zstd_window_log_max));
            break;
        }
    }

    current_path = path_with_info.path;

    if (path_with_info.info && path_with_info.info->size)
    {
        /// Adjust total_rows_approx_accumulated with new total size.
        if (total_files_size)
            total_rows_approx_accumulated = static_cast<size_t>(std::ceil(static_cast<double>(total_files_size + path_with_info.info->size) / total_files_size * total_rows_approx_accumulated));
        total_files_size += path_with_info.info->size;
    }

    input_format = getContext()->getInputFormat(storage->format_name, *read_buf, block_for_format, max_block_size);

    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));
    if (columns_description.hasDefaults())
    {
        builder.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns_description, *input_format, getContext());
        });
    }
    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
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
            Columns columns = chunk.getColumns();
            UInt64 num_rows = chunk.getNumRows();

            if (num_rows && total_files_size)
            {
                size_t chunk_size = input_format->getApproxBytesReadForChunk();
                if (!chunk_size)
                    chunk_size = chunk.bytes();
                updateRowsProgressApprox(*this, num_rows, chunk_size, total_files_size, total_rows_approx_accumulated, total_rows_count_times, total_rows_approx_max);
            }

            for (const auto & virtual_column : requested_virtual_columns)
            {
                if (virtual_column.name == "_path")
                {
                    auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, current_path);
                    columns.push_back(column->convertToFullColumnIfConst());
                }
                else if (virtual_column.name == "_file")
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, std::move(file_name));
                    columns.push_back(column->convertToFullColumnIfConst());
                }
            }

            return Chunk(std::move(columns), num_rows);
        }

        reader.reset();
        pipeline.reset();
        input_format.reset();
        read_buf.reset();

        if (!initialize())
            break;
    }
    return {};
}


class HDFSSink : public SinkToStorage
{
public:
    HDFSSink(const String & uri,
        const String & format,
        const Block & sample_block,
        ContextPtr context,
        const CompressionMethod compression_method)
        : SinkToStorage(sample_block)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::make_unique<WriteBufferFromHDFS>(
                uri,
                context->getGlobalContext()->getConfigRef(),
                context->getSettingsRef().hdfs_replication,
                context->getWriteSettings()),
            compression_method, 3);
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
        return std::make_shared<HDFSSink>(path, format, sample_block, context, compression_method);
    }

private:
    const String uri;
    const String format;
    const Block sample_block;
    ContextPtr context;
    const CompressionMethod compression_method;
};


bool StorageHDFS::supportsSubsetOfColumns() const
{
    return format_name != "Distributed" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name);
}

Pipe StorageHDFS::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    std::shared_ptr<HDFSSource::IteratorWrapper> iterator_wrapper{nullptr};
    if (distributed_processing)
    {
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>(
            [callback = context_->getReadTaskCallback()]() -> StorageHDFS::PathWithInfo {
                return StorageHDFS::PathWithInfo{callback(), std::nullopt};
        });
    }
    else if (is_path_with_globs)
    {
        /// Iterate through disclosed globs and make a source for each file
        auto glob_iterator = std::make_shared<HDFSSource::DisclosedGlobIterator>(context_, uris[0]);
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>([glob_iterator]()
        {
            return glob_iterator->next();
        });
    }
    else
    {
        auto uris_iterator = std::make_shared<HDFSSource::URISIterator>(uris, context_);
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>([uris_iterator]()
        {
            return uris_iterator->next();
        });
    }

    std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
    std::vector<NameAndTypePair> requested_virtual_columns;

    for (const auto & virtual_column : getVirtuals())
    {
        if (column_names_set.contains(virtual_column.name))
            requested_virtual_columns.push_back(virtual_column);
    }

    ColumnsDescription columns_description;
    Block block_for_format;
    if (supportsSubsetOfColumns())
    {
        auto fetch_columns = column_names;
        const auto & virtuals = getVirtuals();
        std::erase_if(
            fetch_columns,
            [&](const String & col)
            { return std::any_of(virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col){ return col == virtual_col.name; }); });

        if (fetch_columns.empty())
            fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()).name);

        columns_description = storage_snapshot->getDescriptionForColumns(fetch_columns);
        block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    }
    else
    {
        columns_description = storage_snapshot->metadata->getColumns();
        block_for_format = storage_snapshot->metadata->getSampleBlock();
    }

    Pipes pipes;
    auto this_ptr = std::static_pointer_cast<StorageHDFS>(shared_from_this());
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<HDFSSource>(
            this_ptr,
            block_for_format,
            requested_virtual_columns,
            context_,
            max_block_size,
            iterator_wrapper,
            columns_description));
    }
    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageHDFS::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
{
    String current_uri = uris.back();

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

        const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(current_uri);

        HDFSBuilderWrapper builder = createHDFSBuilder(uri_without_path + "/", context_->getGlobalContext()->getConfigRef());
        HDFSFSPtr fs = createHDFSFS(builder.get());

        bool truncate_on_insert = context_->getSettingsRef().hdfs_truncate_on_insert;
        if (!truncate_on_insert && !hdfsExists(fs.get(), path_from_uri.c_str()))
        {
            if (context_->getSettingsRef().hdfs_create_new_file_on_insert)
            {
                auto pos = uris[0].find_first_of('.', uris[0].find_last_of('/'));
                size_t index = uris.size();
                String new_uri;
                do
                {
                    new_uri = uris[0].substr(0, pos) + "." + std::to_string(index) + (pos == std::string::npos ? "" : uris[0].substr(pos));
                    ++index;
                }
                while (!hdfsExists(fs.get(), new_uri.c_str()));
                uris.push_back(new_uri);
                current_uri = new_uri;
            }
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "File with path {} already exists. If you want to overwrite it, enable setting hdfs_truncate_on_insert, "
                    "if you want to create new file on each insert, enable setting hdfs_create_new_file_on_insert",
                    path_from_uri);
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
            format_name = FormatFactory::instance().getFormatFromFileName(url, true);

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

NamesAndTypesList StorageHDFS::getVirtuals() const
{
    return virtual_columns;
}

SchemaCache & StorageHDFS::getSchemaCache(const ContextPtr & ctx)
{
    static SchemaCache schema_cache(ctx->getConfigRef().getUInt("schema_inference_cache_max_elements_for_hdfs", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<ColumnsDescription> StorageHDFS::tryGetColumnsFromCache(
    const std::vector<StorageHDFS::PathWithInfo> & paths_with_info,
    const String & uri_without_path,
    const String & format_name,
    const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    for (const auto & path_with_info : paths_with_info)
    {
        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            if (path_with_info.info)
                return path_with_info.info->last_mod_time;
            return std::nullopt;
        };

        String url = fs::path(uri_without_path) / path_with_info.path;
        auto cache_key = getKeyForSchemaCache(url, format_name, {}, ctx);
        auto columns = schema_cache.tryGet(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void StorageHDFS::addColumnsToCache(
    const std::vector<StorageHDFS::PathWithInfo> & paths_with_info,
    const String & uri_without_path,
    const ColumnsDescription & columns,
    const String & format_name,
    const ContextPtr & ctx)
{
    auto & schema_cache = getSchemaCache(ctx);
    Strings sources;
    sources.reserve(paths_with_info.size());
    std::transform(paths_with_info.begin(), paths_with_info.end(), std::back_inserter(sources), [&](const PathWithInfo & path_with_info){ return fs::path(uri_without_path) / path_with_info.path; });
    auto cache_keys = getKeysForSchemaCache(sources, format_name, {}, ctx);
    schema_cache.addMany(cache_keys, columns);
}

}

#endif
