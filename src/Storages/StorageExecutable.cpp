#include <Storages/StorageExecutable.h>
#include <Processors/Pipe.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFile.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Dictionaries/ExecutableDictionarySource.h>
#include <Interpreters/evaluateConstantExpression.cpp>
#include <Core/Block.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace
{
class StorageExecutableSource : public SourceWithProgress
{
public:

    String getName() const override { return "Executable"; }

    StorageExecutableSource(
        const String & /*format*/,
        const Block & sample_block,
        const Context & /*context*/,
        const ColumnsDescription & /*columns*/,
        UInt64 /*max_block_size*/,
        const CompressionMethod /*compression_method*/,
        const String & file_path_)
        : SourceWithProgress(sample_block)
        , file_path(std::move(file_path_))
    {

    }

    Chunk generate() override {
        return Chunk();
    }

    private:

        String file_path;
};
}


StorageExecutable::StorageExecutable(
    const StorageID & table_id,
    const String & format_name_,
    const String & file_path_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    Context & context_,
    CompressionMethod compression_method_)
    : IStorage(table_id)
    , format_name(format_name_)
    , file_path(file_path_)
    , context(context_)
    , compression_method(compression_method_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);
}

void registerStorageExecutable(StorageFactory & factory)
{
    factory.registerStorage(
        "Executable",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;

            if (!(engine_args.size() >= 1 && engine_args.size() <= 4)) // NOLINT
                throw Exception(
                    "Storage Executable requires from 1 to 4 arguments: name of used format, source and compression_method.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
            String format_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

            String compression_method_str;
            String source_path;

            if (const auto * literal = engine_args[1]->as<ASTLiteral>())
            {
                auto type = literal->value.getType();
                if (type == Field::Types::String)
                    source_path = literal->value.get<String>();
                else
                    throw Exception("Second argument must be path or file descriptor", ErrorCodes::BAD_ARGUMENTS);
            }

            if (engine_args.size() == 3)
            {
                engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);
                compression_method_str = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            }

            CompressionMethod compression_method = chooseCompressionMethod("", compression_method_str);

//            if (0 <= source_fd) /// File descriptor
//                return StorageExecutable::create(source_fd, common_args);
//            else /// User's file
            return StorageExecutable::create(args.table_id, format_name, source_path, args.columns, args.constraints, args.context, compression_method);
        },
        {
            .source_access_type = AccessType::FILE,
        });
}



Pipe StorageExecutable::read(
    const Names & /*column_names*/,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    const Context & context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    return Pipe(std::make_shared<StorageExecutableSource>(
        format_name,
        metadata_snapshot->getSampleBlock(),
        context_,
        metadata_snapshot->getColumns(),
        max_block_size,
        compression_method,
        file_path));
}
};

