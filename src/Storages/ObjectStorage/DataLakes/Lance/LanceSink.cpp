#include "config.h"

#if USE_LANCE

#include <Common/Exception.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceSink.h>

#include <arrow/table.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}

LanceSink::LanceSink(SharedHeader sample_block_, Lance::DatasetOptions options)
    : SinkToStorage(sample_block_)
    , sample_block(std::move(sample_block_))
    , writer(Lance::Writer::open(options))
{
    CHColumnToArrowColumn::Settings settings;
    settings.output_string_as_string = true;
    converter = std::make_unique<CHColumnToArrowColumn>(*sample_block, "Lance", settings);
}

LanceSink::~LanceSink() = default;

void LanceSink::consume(Chunk & chunk)
{
    if (isCancelled() || chunk.getNumRows() == 0)
        return;

    std::vector<Chunk> chunks;
    chunks.emplace_back(std::move(chunk));

    std::shared_ptr<arrow::Table> table;
    converter->chChunkToArrowTable(table, chunks, sample_block->columns());

    auto batch = table->CombineChunksToBatch();
    if (!batch.ok())
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to create Arrow batch for Lance write: {}",
            batch.status().ToString());

    writer.writeBatch(**batch);
}

void LanceSink::onFinish()
{
    if (isCancelled())
        return;

    writer.finish();
}

}

#endif
