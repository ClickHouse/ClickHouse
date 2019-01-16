#include <Common/config.h>

#if USE_HDFS

#include <Storages/StorageFactory.h>
#include <Storages/StorageHDFS.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadBufferFromHDFS.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Poco/Path.h>
#include <Common/parseRemoteDescription.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

StorageHDFS::StorageHDFS(const String & uri_,
    const std::string & table_name_,
    const String & format_name_,
    const ColumnsDescription & columns_,
    Context &)
    : IStorage(columns_), uri(uri_), format_name(format_name_), table_name(table_name_)
{
}

namespace
{
    class HDFSBlockInputStream : public IProfilingBlockInputStream
    {
    public:
        HDFSBlockInputStream(const String & uri,
            const String & format,
            const Block & sample_block,
            const Context & context,
            size_t max_block_size)
        {
            // Assume no query and fragment in uri, todo, add sanity check
            String glob_file_names;
            String url_prefix = uri.substr(0, uri.find_last_of('/'));
            if (url_prefix.length() == uri.length())
            {
                glob_file_names = uri;
                url_prefix.clear();
            }
            else
            {
                url_prefix += "/";
                glob_file_names = uri.substr(url_prefix.length());
            }

            std::vector<String> glob_names_list = parseRemoteDescription(glob_file_names, 0, glob_file_names.length(), ',' , 100/* hard coded max files */);

            BlockInputStreams inputs;

            for (const auto & name : glob_names_list)
            {
                std::unique_ptr<ReadBuffer> read_buf = std::make_unique<ReadBufferFromHDFS>(url_prefix + name);

                inputs.emplace_back(
                    std::make_shared<OwningBlockInputStream<ReadBuffer>>(
                        FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size),
                        std::move(read_buf)));
            }

            if (inputs.size() == 0)
                throw Exception("StorageHDFS inputs interpreter error", ErrorCodes::BAD_ARGUMENTS);

            if (inputs.size() == 1)
            {
                reader = inputs[0];
            }
            else
            {
                reader = std::make_shared<UnionBlockInputStream>(inputs, nullptr, context.getSettingsRef().max_distributed_connections);
            }
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
            if (auto concrete_reader = dynamic_cast<UnionBlockInputStream *>(reader.get()))
                concrete_reader->cancel(false); // skip Union read suffix assertion

            reader->readSuffix();
        }

    private:
        BlockInputStreamPtr reader;
    };

}


BlockInputStreams StorageHDFS::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum  /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    return {std::make_shared<HDFSBlockInputStream>(
        uri,
        format_name,
        getSampleBlock(),
        context,
        max_block_size)};
}

void StorageHDFS::rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/) {}

BlockOutputStreamPtr StorageHDFS::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    throw Exception("StorageHDFS write is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
}

void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage("HDFS", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 1 || engine_args.size() == 2))
            throw Exception(
                "Storage HDFS requires exactly 2 arguments: url and name of used format.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);

        String url = static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>();

        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String format_name = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();

        return StorageHDFS::create(url, args.table_name, format_name, args.columns, args.context);
    });
}

}

#endif
