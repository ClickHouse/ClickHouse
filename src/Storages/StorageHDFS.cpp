#include <Common/config.h>

#if USE_HDFS

#include <Storages/StorageFactory.h>
#include <Storages/StorageHDFS.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromHDFS.h>
#include <IO/WriteBufferFromHDFS.h>
#include <IO/WriteHelpers.h>
#include <IO/HDFSCommon.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>

#include <Common/parseGlobs.h>
#include <Poco/URI.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <hdfs/hdfs.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_PARTITION_VALUE;
}

StorageHDFS::StorageHDFS(const String & uri_,
    const StorageID & table_id_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const ASTPtr & partition_by_ast_,
    Context & context_,
    const String & compression_method_ = "")
    : IStorage(table_id_)
    , uri(uri_)
    , format_name(format_name_)
    , partition_by_ast(partition_by_ast_)
    , context(context_)
    , compression_method(compression_method_)
{
    context.getRemoteHostFilter().checkURL(Poco::URI(uri));

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);

    if (partition_by_ast)
    {
        storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, columns_, context);

        partition_name_types = storage_metadata.partition_key.expression->getRequiredColumnsWithTypes();
        minmax_idx_expr = std::make_shared<ExpressionActions>(partition_name_types, context);
    }
    setInMemoryMetadata(storage_metadata);
}


namespace
{

class HDFSSource : public SourceWithProgress
{
public:

    struct SourcesInfo
    {
        std::vector<String> uris;
        std::vector<FieldVector> partition_fields;

        NamesAndTypesList partition_name_types;

        std::atomic<size_t> next_uri_to_read = 0;

        bool need_path_column = false;
        bool need_file_column = false;
    };

    using SourcesInfoPtr = std::shared_ptr<SourcesInfo>;

    static Block getHeader(Block header, const SourcesInfoPtr & source_info)
    {
        if (source_info->need_path_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
        if (source_info->need_file_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

        return header;
    }

    HDFSSource(
        SourcesInfoPtr source_info_,
        String uri_,
        String format_,
        String compression_method_,
        Block sample_block_,
        const Context & context_,
        UInt64 max_block_size_)
        : SourceWithProgress(getHeader(sample_block_, source_info_))
        , source_info(std::move(source_info_))
        , uri(std::move(uri_))
        , format(std::move(format_))
        , compression_method(compression_method_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
        , context(context_)
    {
    }

    String getName() const override
    {
        return "HDFS";
    }

    Chunk generate() override
    {
        std::vector<size_t> partition_indexs;
        auto to_read_block = sample_block;
        for (const auto & name_type : source_info->partition_name_types)
        {
            to_read_block.erase(name_type.name);
        }

        while (true)
        {
            if (!reader)
            {
                current_idx = source_info->next_uri_to_read.fetch_add(1);
                if (current_idx >= source_info->uris.size())
                    return {};

                auto path =  source_info->uris[current_idx];
                current_path = uri + path;

                auto compression = chooseCompressionMethod(path, compression_method);
                auto read_buf = wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromHDFS>(current_path), compression);
                auto input_stream = FormatFactory::instance().getInput(format, *read_buf, to_read_block, context, max_block_size);

                reader = std::make_shared<OwningBlockInputStream<ReadBuffer>>(input_stream, std::move(read_buf));
                reader->readPrefix();
            }

            if (auto res = reader->read())
            {
                Columns columns = res.getColumns();
                UInt64 num_rows = res.rows();

                auto types = source_info->partition_name_types.getTypes();
                for (size_t i = 0; i < types.size(); ++i)
                {
                    auto column = types[i]->createColumnConst(num_rows, source_info->partition_fields[current_idx][i]);
                    auto previous_idx = sample_block.getPositionByName(source_info->partition_name_types.getNames()[i]);
                    columns.insert(columns.begin() + previous_idx, column->convertToFullColumnIfConst());
                }

                  /// Enrich with virtual columns.
                if (source_info->need_path_column)
                {
                    auto column = DataTypeString().createColumnConst(num_rows, current_path);
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                if (source_info->need_file_column)
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column = DataTypeString().createColumnConst(num_rows, std::move(file_name));
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                return Chunk(std::move(columns), num_rows);
            }

            reader->readSuffix();
            reader.reset();
        }
    }

private:
    BlockInputStreamPtr reader;
    SourcesInfoPtr source_info;
    String uri;
    String format;
    String compression_method;
    String current_path;
    size_t current_idx;

    UInt64 max_block_size;
    Block sample_block;

    const Context & context;
};

class HDFSBlockOutputStream : public IBlockOutputStream
{
public:
    HDFSBlockOutputStream(const String & uri,
        const String & format,
        const Block & sample_block_,
        const Context & context,
        const CompressionMethod compression_method)
        : sample_block(sample_block_)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(std::make_unique<WriteBufferFromHDFS>(uri), compression_method, 3);
        writer = FormatFactory::instance().getOutput(format, *write_buf, sample_block, context);
    }

    Block getHeader() const override
    {
        return sample_block;
    }

    void write(const Block & block) override
    {
        writer->write(block);
    }

    void writePrefix() override
    {
        writer->writePrefix();
    }

    void writeSuffix() override
    {
        writer->writeSuffix();
        writer->flush();
        write_buf->sync();
    }

private:
    Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    BlockOutputStreamPtr writer;
};

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
    Strings result;
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
                Strings result_part = LSWithRegexpMatching(full_path + "/", fs, suffix_with_globs.substr(next_slash));
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
            }
        }
    }

    return result;
}

}

Pipe StorageHDFS::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    const size_t begin_of_path = uri.find('/', uri.find("//") + 2);
    const String uri_without_path = uri.substr(0, begin_of_path);
    const String path_from_uri = uri.substr(begin_of_path);

    HDFSBuilderPtr builder = createHDFSBuilder(uri_without_path + "/");
    HDFSFSPtr fs = createHDFSFS(builder.get());

    auto sources_info = std::make_shared<HDFSSource::SourcesInfo>();
    sources_info->uris = LSWithRegexpMatching("/", fs, path_from_uri);

    if (minmax_idx_expr)
    {
        const auto names = partition_name_types.getNames();
        std::optional<KeyCondition> minmax_idx_condition;
        minmax_idx_condition.emplace(query_info, context, names, minmax_idx_expr);
        auto prev_uris = sources_info->uris;
        sources_info->uris.clear();
        sources_info->partition_name_types = partition_name_types;

        for (const auto & s_uri : prev_uris)
        {
            re2::StringPiece input(s_uri);
            String tmp;
            WriteBufferFromOwnString wb;

            // consume the partition value from uri by regexp
            // todo: integration with hive metastore
            for (size_t i = 0; i < names.size(); ++i)
            {
                if (!RE2::FindAndConsume(&input, "/" + names[i] + "=([^/]+)", &tmp))
                    throw Exception(
                    "Could not parse partition file path: " + s_uri,
                    ErrorCodes::INVALID_PARTITION_VALUE);

                if (i != 0)
                    writeString(",", wb);

                writeString(tmp, wb);
            }

            // TODO: CSV is a little hacky, maybe some better way? eg: Values
            ReadBufferFromString buffer(wb.str());

            auto input_stream
                = FormatFactory::instance().getInput("CSV", buffer, metadata_snapshot->getPartitionKey().sample_block, context, context.getSettingsRef().max_block_size);

            auto block = input_stream->read();
            if (!block || !block.rows())
                throw Exception(
                    "Could not parse partition value: `",
                    ErrorCodes::INVALID_PARTITION_VALUE);

            std::vector<Field> fields(names.size());
            std::vector<Range> ranges;
            for (size_t i = 0; i < names.size(); ++i)
            {
                block.getByPosition(i).column->get(0, fields[i]);
                ranges.emplace_back(fields[i]);
            }

            if (minmax_idx_condition->checkInHyperrectangle(ranges, partition_name_types.getTypes()).can_be_true)
            {
                LOG_INFO(log, "matched partition: {}, hdfs file: {}",  partition_name_types.toString(), s_uri);
                sources_info->uris.push_back(s_uri);
                sources_info->partition_fields.push_back(std::move(fields));
            }
        }
    }

    for (const auto & column : column_names)
    {
        if (column == "_path")
            sources_info->need_path_column = true;
        if (column == "_file")
            sources_info->need_file_column = true;
    }

    if (num_streams > sources_info->uris.size())
        num_streams = sources_info->uris.size();

    Pipes pipes;

    for (size_t i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<HDFSSource>(
                sources_info, uri_without_path, format_name, compression_method, metadata_snapshot->getSampleBlock(), context_, max_block_size));

    return Pipe::unitePipes(std::move(pipes));
}

BlockOutputStreamPtr StorageHDFS::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<HDFSBlockOutputStream>(uri,
        format_name,
        metadata_snapshot->getSampleBlock(),
        context,
        chooseCompressionMethod(uri, compression_method));
}

void registerStorageHDFS(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .source_access_type = AccessType::HDFS
    };

    factory.registerStorage("HDFS", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2 && engine_args.size() != 3)
            throw Exception(
                "Storage HDFS requires 2 or 3 arguments: url, name of used format and optional compression method.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        String compression_method;
        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);
            compression_method = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        } else compression_method = "auto";

        ASTPtr partition_by_ast;
        if (args.storage_def->partition_by)
            partition_by_ast = args.storage_def->partition_by->ptr();

        return StorageHDFS::create(url, args.table_id, format_name, args.columns, args.constraints, partition_by_ast, args.context, compression_method);
    }, features);
}

// Though partition cols is virtual column of hdfs storage
// but we can consider it as material column in ClickHouse
NamesAndTypesList StorageHDFS::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}
}

#endif
