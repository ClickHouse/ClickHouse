#include <Common/config.h>

#if USE_HDFS

#include <Storages/StorageFactory.h>
#include <Storages/StorageHDFS.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromHDFS.h>
#include <IO/WriteBufferFromHDFS.h>
#include <IO/WriteHelpers.h>
#include <IO/HDFSCommon.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>

#include <Common/parseGlobs.h>
#include <Poco/URI.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <hdfs/hdfs.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

StorageHDFS::StorageHDFS(const String & uri_,
    const std::string & database_name_,
    const std::string & table_name_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    Context & context_,
    const String & compression_method_ = "")
    : uri(uri_)
    , format_name(format_name_)
    , table_name(table_name_)
    , database_name(database_name_)
    , context(context_)
    , compression_method(compression_method_)
{
    context.getRemoteHostFilter().checkURL(Poco::URI(uri));
    setColumns(columns_);
    setConstraints(constraints_);
}

namespace
{

class HDFSBlockInputStream : public IBlockInputStream
{
public:
    HDFSBlockInputStream(const String & uri,
        const String & format,
        const Block & sample_block,
        const Context & context,
        UInt64 max_block_size,
        const CompressionMethod compression_method)
    {
        auto read_buf = getReadBuffer<ReadBufferFromHDFS>(compression_method, uri);

        auto input_stream = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);
        reader = std::make_shared<OwningBlockInputStream<ReadBuffer>>(input_stream, std::move(read_buf));
    }

    String getName() const override
    {
        return "HDFS";
    }

    Block readImpl() override
    {
        return reader->read();
    }

    Block getHeader() const override
    {
        return reader->getHeader();
    }

    void readPrefixImpl() override
    {
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();
    }

private:
    BlockInputStreamPtr reader;
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
        write_buf = getWriteBuffer<WriteBufferFromHDFS>(compression_method, uri);
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


BlockInputStreams StorageHDFS::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context_,
    QueryProcessingStage::Enum  /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    const size_t begin_of_path = uri.find('/', uri.find("//") + 2);
    const String path_from_uri = uri.substr(begin_of_path);
    const String uri_without_path = uri.substr(0, begin_of_path);

    HDFSBuilderPtr builder = createHDFSBuilder(uri_without_path + "/");
    HDFSFSPtr fs = createHDFSFS(builder.get());

    const Strings res_paths = LSWithRegexpMatching("/", fs, path_from_uri);
    BlockInputStreams result;
    for (const auto & res_path : res_paths)
    {
        result.push_back(std::make_shared<HDFSBlockInputStream>(uri_without_path + res_path, format_name, getSampleBlock(), context_,
                                                               max_block_size, IStorage::chooseCompressionMethod(res_path, compression_method)));
    }

    return narrowBlockInputStreams(result, num_streams);
}

void StorageHDFS::rename(const String & /*new_path_to_db*/, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &)
{
    table_name = new_table_name;
    database_name = new_database_name;
}

BlockOutputStreamPtr StorageHDFS::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<HDFSBlockOutputStream>(uri,
        format_name,
        getSampleBlock(),
        context,
        IStorage::chooseCompressionMethod(uri, compression_method));
}

void registerStorageHDFS(StorageFactory & factory)
{
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

        return StorageHDFS::create(url, args.database_name, args.table_name, format_name, args.columns, args.constraints, args.context, compression_method);
    });
}

}

#endif
