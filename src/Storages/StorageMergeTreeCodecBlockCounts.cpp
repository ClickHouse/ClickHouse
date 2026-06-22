#include <Storages/StorageMergeTreeCodecBlockCounts.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/IColumn.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/getCompressionCodecForFile.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace
{

const String PART_NAME_COLUMN = "part_name";
const String COLUMN_COLUMN = "column";
const String SUBSTREAM_COLUMN = "substream";
const String CODEC_BLOCK_COUNTS_COLUMN = "codec_block_counts";

Map codecCountsToField(const std::map<String, UInt64> & counts)
{
    Map result;
    result.reserve(counts.size());
    for (const auto & [name, count] : counts)
        result.emplace_back(Tuple({name, count}));
    return result;
}

class MergeTreeCodecBlockCountsSource final : public ISource
{
public:
    MergeTreeCodecBlockCountsSource(SharedHeader header_, MergeTreeData::DataPartsVector data_parts_, ReadSettings read_settings_)
        : ISource(header_)
        , header(std::move(header_))
        , data_parts(std::move(data_parts_))
        , read_settings(std::move(read_settings_))
    {
        need_counts = header->has(CODEC_BLOCK_COUNTS_COLUMN);
    }

    String getName() const override { return "MergeTreeCodecBlockCounts"; }

protected:
    Chunk generate() override
    {
        /// One part per call. Skip parts with empty ColumnsSubstreams (e.g. Compact without substream marks), else getColumnSubstreams throws.
        while (part_index < data_parts.size())
        {
            const auto & part = data_parts[part_index++];
            const auto & columns_substreams = part->getColumnsSubstreams();
            if (columns_substreams.empty())
                continue;

            MutableColumns result(header->columns());
            for (size_t pos = 0; pos < header->columns(); ++pos)
                result[pos] = header->getByPosition(pos).type->createColumn();

            size_t num_rows = 0;
            size_t column_position = 0;
            for (const auto & column : part->getColumns())
            {
                for (const auto & substream : columns_substreams.getColumnSubstreams(column_position))
                {
                    Map codec_counts;
                    if (need_counts)
                        codec_counts = computeForSubstream(part, substream);

                    for (size_t pos = 0; pos < header->columns(); ++pos)
                    {
                        const auto & name = header->getByPosition(pos).name;
                        if (name == PART_NAME_COLUMN)
                            result[pos]->insert(part->name);
                        else if (name == COLUMN_COLUMN)
                            result[pos]->insert(column.name);
                        else if (name == SUBSTREAM_COLUMN)
                            result[pos]->insert(substream);
                        else if (name == CODEC_BLOCK_COUNTS_COLUMN)
                            result[pos]->insert(codec_counts);
                        else
                            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "No such column: {}", name);
                    }
                    ++num_rows;
                }
                ++column_position;
            }

            if (num_rows == 0)
                continue;

            return Chunk(std::move(result), num_rows);
        }

        return {};
    }

private:
    /// Counts the codec of every compressed block in the substream's `.bin`. Returns {} for Compact parts.
    /// Their columns share one `data.bin` (not per substream), and a compressed block can span several substreams.
    Map computeForSubstream(const MergeTreeDataPartPtr & part, const String & substream)
    {
        if (isCompactPart(part))
            return {};

        auto filename = IMergeTreeDataPart::getStreamNameOrHash(substream, ".bin", part->checksums);
        if (!filename)
            return {};

        auto read_buffer = part->getDataPartStorage().readFile(*filename + ".bin", read_settings, std::nullopt);
        std::map<String, UInt64> counts;
        while (!read_buffer->eof())
        {
            UInt32 size_compressed = 0;
            UInt32 size_decompressed = 0;
            auto codec = getCompressionCodecForFile(*read_buffer, size_compressed, size_decompressed, true);
            ++counts[codec->getCodecDesc()->formatForLogging()];
        }
        return codecCountsToField(counts);
    }

    SharedHeader header;
    MergeTreeData::DataPartsVector data_parts;
    ReadSettings read_settings;

    bool need_counts = false;
    size_t part_index = 0;
};

}

StorageMergeTreeCodecBlockCounts::StorageMergeTreeCodecBlockCounts(
    const StorageID & table_id_, StoragePtr source_table_, const ColumnsDescription & columns_)
    : IStorage(table_id_)
    , source_table(std::move(source_table_))
{
    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeCodecBlockCounts expected MergeTree table, got: {}", source_table->getName());

    data_parts = merge_tree->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

/// TODO: WHERE pushdown. Right now, `SELECT ... WHERE part_name = 'p'` reads every part's `.bin` and filters rows after the read.
Pipe StorageMergeTreeCodecBlockCounts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    const auto source_metadata = source_table->getInMemoryMetadataPtr(context, false);
    context->checkAccess(AccessType::SELECT, source_table->getStorageID(), source_metadata->getColumns().getNamesOfPhysical());

    auto sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    return Pipe(std::make_shared<MergeTreeCodecBlockCountsSource>(std::move(sample_block), data_parts, context->getReadSettings()));
}

}
