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
#include <DataStreams/ShellCommandBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
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
        const String & file_path_,
        const String & format_,
        BlockInputStreamPtr input_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Context & context_,
        UInt64 max_block_size_)
        : SourceWithProgress(metadata_snapshot_->getSampleBlock())
        , file_path(std::move(file_path_))
        , format(format_)
        , input(input_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , max_block_size(max_block_size_)
    {
    }

    Chunk generate() override {
        LOG_TRACE(log, "generating {}", toString(file_path));

        if (!reader)
        {
            auto sample_block = metadata_snapshot->getSampleBlock();
            reader = std::make_shared<BlockInputStreamWithBackgroundThread>(context, format, sample_block, file_path, log, max_block_size, [this](WriteBufferFromFile & out)
            {
                if (!input)
                {
                    out.close();
                    return;
                }

                auto output_stream = context.getOutputFormat(format, out, input->getHeader());
                output_stream->writePrefix();

                input->readPrefix();
                while (auto block = input->read()) {
                    output_stream->write(block);
                }
                input->readSuffix();
                input.reset();

                output_stream->writeSuffix();
                output_stream->flush();
                out.close();
            });

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
        const String & format;
        BlockInputStreamPtr input;
        BlockInputStreamPtr reader;
        const StorageMetadataPtr & metadata_snapshot;
        const Context & context;
        UInt64 max_block_size;
        Poco::Logger * log = &Poco::Logger::get("StorageExecutableSource");
};
}


StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & file_path_,
    const String & format_,
    BlockInputStreamPtr input_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const Context & context_)
    : IStorage(table_id_)
    , file_path(file_path_)
    , format(format_)
    , input(input_)
    , context(context_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);
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
        file_path,
        format,
        input,
        metadata_snapshot,
        context_,
        max_block_size));
}
};

