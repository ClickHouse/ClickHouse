#include <Storages/StorageExecutable.h>
#include <Processors/Pipe.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFile.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFactory.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <DataStreams/ShellCommandOwningBlockInputStream.h>
#include <Dictionaries/ExecutableDictionarySource.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Block.h>
#include <Common/ShellCommand.h>

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
        const String & format_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Context & context_,
        const ColumnsDescription & /*columns*/,
        UInt64 max_block_size_,
        const CompressionMethod /*compression_method*/,
        const String & file_path_)
        : SourceWithProgress(metadata_snapshot_->getSampleBlock())
        , file_path(std::move(file_path_))
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , format(format_)
        , max_block_size(max_block_size_)
    {
    }

    Chunk generate() override {
        LOG_TRACE(log, "generating {}", toString(file_path));

        if (!reader)
        {
            auto process = ShellCommand::execute(file_path);
            auto input_stream = context.getInputFormat(format, process->out, metadata_snapshot->getSampleBlock(), max_block_size);
            reader = std::make_shared<ShellCommandOwningBlockInputStream>(log, input_stream, std::move(process));

            reader->readPrefix();
        }

        if (auto res = reader->read())
        {
            Columns columns = res.getColumns();
            UInt64 num_rows = res.rows();
            return Chunk(std::move(columns), num_rows);
        }

        reader->readSuffix();
        reader.reset();
        return {};
    }

    private:
        String file_path;
        BlockInputStreamPtr reader;
        const StorageMetadataPtr & metadata_snapshot;
        const Context & context;
        const String & format;
        UInt64 max_block_size;
        Poco::Logger * log = &Poco::Logger::get("StorageExecutableSource");
};
}


StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & format_name_,
    const String & file_path_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    Context & context_,
    CompressionMethod compression_method_)
    : IStorage(table_id_)
    , format_name(format_name_)
    , file_path(file_path_)
    , context(context_)
    , compression_method(compression_method_)
    , log(&Poco::Logger::get("StorageExecutable (" + table_id_.getFullTableName() + ")"))
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
        metadata_snapshot,
        context_,
        metadata_snapshot->getColumns(),
        max_block_size,
        compression_method,
        file_path));
}
};

