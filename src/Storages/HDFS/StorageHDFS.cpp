#include <Common/config.h>

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

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Storages/StorageFactory.h>
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/PartitionedSink.h>

#include <Formats/ReadSchemaUtils.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionsConversion.h>

#include <QueryPipeline/QueryPipeline.h>
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
}
namespace
{
    /* Recursive directory listing with matched paths as a result.
     * Have the same method in StorageFile.
     */
    Strings LSWithRegexpMatching(const String & path_for_ls, const HDFSFSPtr & fs, const String & for_match)
    {
        const size_t first_glob = for_match.find_first_of("*?{");

        const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
        const String suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'
        const String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'

        const size_t next_slash = suffix_with_globs.find('/', 1);
        re2::RE2 matcher(makeRegexpPatternFromGlobs(suffix_with_globs.substr(0, next_slash)));

        HDFSFileInfo ls;
        ls.file_info = hdfsListDirectory(fs.get(), prefix_without_globs.data(), &ls.length);
        if (ls.file_info == nullptr && errno != ENOENT) // NOLINT
        {
            // ignore file not found exception, keep throw other exception, libhdfs3 doesn't have function to get exception type, so use errno.
            throw Exception(
                ErrorCodes::ACCESS_DENIED, "Cannot list directory {}: {}", prefix_without_globs, String(hdfsGetLastError()));
        }
        Strings result;
        if (!ls.file_info && ls.length > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "file_info shouldn't be null");
        for (int i = 0; i < ls.length; ++i)
        {
            const String full_path = String(ls.file_info[i].mName);
            const size_t last_slash = full_path.rfind('/');
            const String file_name = full_path.substr(last_slash);
            const bool looking_for_directory = next_slash != std::string::npos;
            const bool is_directory = ls.file_info[i].mKind == 'D';
            /// Condition with type of current file_info means what kind of path is it in current iteration of ls
            if (!is_directory && !looking_for_directory)
            {
                if (re2::RE2::FullMatch(file_name, matcher))
                {
                    result.push_back(String(ls.file_info[i].mName));
                }
            }
            else if (is_directory && looking_for_directory)
            {
                if (re2::RE2::FullMatch(file_name, matcher))
                {
                    Strings result_part = LSWithRegexpMatching(fs::path(full_path) / "", fs, suffix_with_globs.substr(next_slash));
                    /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                    std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
                }
            }
        }

        return result;
    }

    std::pair<String, String> getPathFromUriAndUriWithoutPath(const String & uri)
    {
        const size_t begin_of_path = uri.find('/', uri.find("//") + 2);
        return {uri.substr(begin_of_path), uri.substr(0, begin_of_path)};
    }

    std::vector<String> getPathsList(const String & path_from_uri, const String & uri_without_path, ContextPtr context)
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
}

ColumnsDescription StorageHDFS::getTableStructureFromData(
    const String & format,
    const String & uri,
    const String & compression_method,
    ContextPtr ctx)
{
    const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);
    auto paths = getPathsList(path_from_uri, uri, ctx);

    std::string exception_messages;
    bool read_buffer_creator_was_used = false;
    for (const auto & path : paths)
    {
        auto read_buffer_creator = [&, uri_without_path = uri_without_path]()
        {
            read_buffer_creator_was_used = true;

            if (paths.empty())
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file, because there are no files in HDFS with provided path. You must "
                    "specify table structure manually",
                    format);

            auto compression = chooseCompressionMethod(path, compression_method);
            return wrapReadBufferWithCompressionMethod(
                std::make_unique<ReadBufferFromHDFS>(uri_without_path, path, ctx->getGlobalContext()->getConfigRef()), compression);
        };

        try
        {
            return readSchemaFromFormat(format, std::nullopt, read_buffer_creator, ctx);
        }
        catch (...)
        {
            if (paths.size() == 1 || !read_buffer_creator_was_used)
                throw;

           exception_messages += getCurrentExceptionMessage(false) + "\n";
        }
    }

    throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "All attempts to extract table structure from hdfs files failed. Errors:\n{}", exception_messages);
}

class HDFSSource::DisclosedGlobIterator::Impl
{
public:
    Impl(ContextPtr context_, const String & uri)
    {
        const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(uri);
        uris = getPathsList(path_from_uri, uri_without_path, context_);
        for (auto & elem : uris)
            elem = uri_without_path + elem;
        uris_iter = uris.begin();
    }

    String next()
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
    Strings uris;
    Strings::iterator uris_iter;
};

class HDFSSource::URISIterator::Impl
{
public:
    explicit Impl(const std::vector<const String> & uris_, ContextPtr context)
    {
        auto path_and_uri = getPathFromUriAndUriWithoutPath(uris_[0]);
        HDFSBuilderWrapper builder = createHDFSBuilder(path_and_uri.second + "/", context->getGlobalContext()->getConfigRef());
        HDFSFSPtr fs = createHDFSFS(builder.get());
        for (const auto & uri : uris_)
        {
            path_and_uri = getPathFromUriAndUriWithoutPath(uri);
            if (!hdfsExists(fs.get(), path_and_uri.first.c_str()))
                uris.push_back(uri);
        }
        uris_iter = uris.begin();
    }

    String next()
    {
        std::lock_guard lock(mutex);
        if (uris_iter == uris.end())
            return "";
        auto key = *uris_iter;
        ++uris_iter;
        return key;
    }

private:
    std::mutex mutex;
    Strings uris;
    Strings::iterator uris_iter;
};

Block HDFSSource::getHeader(const StorageMetadataPtr & metadata_snapshot, bool need_path_column, bool need_file_column)
{
    auto header = metadata_snapshot->getSampleBlock();
    /// Note: AddingDefaultsBlockInputStream doesn't change header.
    if (need_path_column)
        header.insert(
            {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
             std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
             "_path"});
    if (need_file_column)
        header.insert(
            {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
             std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
             "_file"});
    return header;
}

Block HDFSSource::getBlockForSource(
    const StorageHDFSPtr & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ColumnsDescription & columns_description,
    bool need_path_column,
    bool need_file_column)
{
    if (storage->isColumnOriented())
        return storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
    else
        return getHeader(storage_snapshot->metadata, need_path_column, need_file_column);
}

HDFSSource::DisclosedGlobIterator::DisclosedGlobIterator(ContextPtr context_, const String & uri)
    : pimpl(std::make_shared<HDFSSource::DisclosedGlobIterator::Impl>(context_, uri)) {}

String HDFSSource::DisclosedGlobIterator::next()
{
    return pimpl->next();
}

HDFSSource::URISIterator::URISIterator(const std::vector<const String> & uris_, ContextPtr context)
    : pimpl(std::make_shared<HDFSSource::URISIterator::Impl>(uris_, context))
{
}

String HDFSSource::URISIterator::next()
{
    return pimpl->next();
}

HDFSSource::HDFSSource(
    StorageHDFSPtr storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    UInt64 max_block_size_,
    bool need_path_column_,
    bool need_file_column_,
    std::shared_ptr<IteratorWrapper> file_iterator_,
    ColumnsDescription columns_description_)
    : SourceWithProgress(getBlockForSource(storage_, storage_snapshot_, columns_description_, need_path_column_, need_file_column_))
    , WithContext(context_)
    , storage(std::move(storage_))
    , storage_snapshot(storage_snapshot_)
    , max_block_size(max_block_size_)
    , need_path_column(need_path_column_)
    , need_file_column(need_file_column_)
    , file_iterator(file_iterator_)
    , columns_description(std::move(columns_description_))
{
    initialize();
}

void HDFSSource::onCancel()
{
    std::lock_guard lock(reader_mutex);
    if (reader)
        reader->cancel();
}

bool HDFSSource::initialize()
{
    current_path = (*file_iterator)();
    if (current_path.empty())
        return false;

    const auto [path_from_uri, uri_without_path] = getPathFromUriAndUriWithoutPath(current_path);

    auto compression = chooseCompressionMethod(path_from_uri, storage->compression_method);
    read_buf = wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromHDFS>(uri_without_path, path_from_uri, getContext()->getGlobalContext()->getConfigRef()), compression);

    auto get_block_for_format = [&]() -> Block
    {
        if (storage->isColumnOriented())
            return storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
        return storage_snapshot->metadata->getSampleBlock();
    };

    auto input_format = getContext()->getInputFormat(storage->format_name, *read_buf, get_block_for_format(), max_block_size);

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
        if (!reader || isCancelled())
            break;

        Chunk chunk;
        if (reader->pull(chunk))
        {
            Columns columns = chunk.getColumns();
            UInt64 num_rows = chunk.getNumRows();

            /// Enrich with virtual columns.
            if (need_path_column)
            {
                auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, current_path);
                columns.push_back(column->convertToFullColumnIfConst());
            }

            if (need_file_column)
            {
                size_t last_slash_pos = current_path.find_last_of('/');
                auto file_name = current_path.substr(last_slash_pos + 1);

                auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, std::move(file_name));
                columns.push_back(column->convertToFullColumnIfConst());
            }

            return Chunk(std::move(columns), num_rows);
        }

        {
            std::lock_guard lock(reader_mutex);
            reader.reset();
            pipeline.reset();
            read_buf.reset();

            if (!initialize())
                break;
        }
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
        write_buf = wrapWriteBufferWithCompressionMethod(std::make_unique<WriteBufferFromHDFS>(uri, context->getGlobalContext()->getConfigRef(), context->getSettingsRef().hdfs_replication), compression_method, 3);
        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context);
    }

    String getName() const override { return "HDFSSink"; }

    void consume(Chunk chunk) override
    {
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onFinish() override
    {
        try
        {
            writer->finalize();
            writer->flush();
            write_buf->sync();
            write_buf->finalize();
        }
        catch (...)
        {
            writer.reset();
            throw;
        }
    }

private:
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
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


bool StorageHDFS::isColumnOriented() const
{
    return format_name != "Distributed" && FormatFactory::instance().checkIfFormatIsColumnOriented(format_name);
}

Pipe StorageHDFS::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    bool need_path_column = false;
    bool need_file_column = false;

    for (const auto & column : column_names)
    {
        if (column == "_path")
            need_path_column = true;
        if (column == "_file")
            need_file_column = true;
    }

    std::shared_ptr<HDFSSource::IteratorWrapper> iterator_wrapper{nullptr};
    if (distributed_processing)
    {
        iterator_wrapper = std::make_shared<HDFSSource::IteratorWrapper>(
            [callback = context_->getReadTaskCallback()]() -> String {
                return callback();
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

    Pipes pipes;
    auto this_ptr = std::static_pointer_cast<StorageHDFS>(shared_from_this());
    for (size_t i = 0; i < num_streams; ++i)
    {
         const auto get_columns_for_format = [&]() -> ColumnsDescription
        {
            if (isColumnOriented())
                return ColumnsDescription{storage_snapshot->getSampleBlockForColumns(column_names).getNamesAndTypesList()};
            else
                return storage_snapshot->metadata->getColumns();
        };

        pipes.emplace_back(std::make_shared<HDFSSource>(
            this_ptr,
            storage_snapshot,
            context_,
            max_block_size,
            need_path_column,
            need_file_column,
            iterator_wrapper,
            get_columns_for_format()));
    }
    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageHDFS::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_)
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
    HDFSFSPtr fs = createHDFSFS(builder.get());

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
            throw Exception(
                "Storage HDFS requires 1, 2 or 3 arguments: url, name of used format (taken from file extension by default) and optional compression method.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.getLocalContext());

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        String format_name = "auto";
        if (engine_args.size() > 1)
        {
            engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());
            format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        }

        if (format_name == "auto")
            format_name = FormatFactory::instance().getFormatFromFileName(url, true);

        String compression_method;
        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.getLocalContext());
            compression_method = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        } else compression_method = "auto";

        ASTPtr partition_by;
        if (args.storage_def->partition_by)
            partition_by = args.storage_def->partition_by->clone();

        return StorageHDFS::create(
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
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}

}

#endif
