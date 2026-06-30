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
}

namespace
{

const String PART_NAME_COLUMN = "part_name";
const String COLUMN_COLUMN = "column";
const String SUBSTREAM_COLUMN = "substream";
const String DATA_COMPRESSED_BYTES_COLUMN = "data_compressed_bytes";
const String DATA_UNCOMPRESSED_BYTES_COLUMN = "data_uncompressed_bytes";
const String CODEC_BLOCK_COUNTS_COLUMN = "codec_block_counts";

Map codecCountsToField(const std::map<String, UInt64> & counts)
{
    Map result;
    result.reserve(counts.size());
    for (const auto & [name, count] : counts)
        result.emplace_back(Tuple({name, count}));
    return result;
}

struct SubstreamInfo
{
    std::optional<UInt64> compressed;
    std::optional<UInt64> uncompressed;
    Map codecs;
};

class MergeTreeCodecBlockCountsSource final : public ISource
{
public:
    MergeTreeCodecBlockCountsSource(SharedHeader header_, MergeTreeData::DataPartsVector data_parts_, ReadSettings read_settings_)
        : ISource(header_)
        , header(std::move(header_))
        , data_parts(std::move(data_parts_))
        , read_settings(std::move(read_settings_))
    {
        auto position_of = [&](const String & name) -> std::optional<size_t>
        { return header->has(name) ? std::optional<size_t>(header->getPositionByName(name)) : std::nullopt; };

        part_name_pos = position_of(PART_NAME_COLUMN);
        column_pos = position_of(COLUMN_COLUMN);
        substream_pos = position_of(SUBSTREAM_COLUMN);
        compressed_pos = position_of(DATA_COMPRESSED_BYTES_COLUMN);
        uncompressed_pos = position_of(DATA_UNCOMPRESSED_BYTES_COLUMN);
        codec_counts_pos = position_of(CODEC_BLOCK_COUNTS_COLUMN);
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
            /// TODO: mutating an old Wide part that has no columns_substreams.txt leaves the rewritten part without one too.
            /// Yet the rewritten columns may hold adaptively-selected codecs. This part is omitted here until next merge records substreams.
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
                    SubstreamInfo info = computeForSubstream(part, substream);

                    if (part_name_pos)
                        result[*part_name_pos]->insert(part->name);
                    if (column_pos)
                        result[*column_pos]->insert(column.name);
                    if (substream_pos)
                        result[*substream_pos]->insert(substream);
                    if (codec_counts_pos)
                        result[*codec_counts_pos]->insert(info.codecs);

                    /// Sizes are Nullable: the value, or a NULL `Field` for Compact / no `.bin`.
                    if (compressed_pos)
                        result[*compressed_pos]->insert(info.compressed ? Field(*info.compressed) : Field());
                    if (uncompressed_pos)
                        result[*uncompressed_pos]->insert(info.uncompressed ? Field(*info.uncompressed) : Field());

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
    /// Per-substream sizes (metadata only) and, when requested, codec block counts (reads `.bin`).
    /// Empty for Compact parts: their columns share one `data.bin`, so there is no per-stream `.bin`.
    SubstreamInfo computeForSubstream(const MergeTreeDataPartPtr & part, const String & substream)
    {
        if (isCompactPart(part))
            return {};

        auto filename = IMergeTreeDataPart::getStreamNameOrHash(substream, ".bin", part->checksums);
        if (!filename)
            return {};

        SubstreamInfo info;
        if (auto it = part->checksums.files.find(*filename + ".bin"); it != part->checksums.files.end())
        {
            info.compressed = it->second.file_size;
            info.uncompressed = it->second.uncompressed_size;
        }

        /// Gate this because the `.bin` walk is expensive.
        if (codec_counts_pos)
            info.codecs = walkBin(part, *filename);

        return info;
    }

    /// Counts the codec of every compressed block in the resolved `.bin` (reads the data file).
    Map walkBin(const MergeTreeDataPartPtr & part, const String & filename)
    {
        auto read_buffer = part->getDataPartStorage().readFile(filename + ".bin", read_settings, std::nullopt);
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

    std::optional<size_t> part_name_pos;
    std::optional<size_t> column_pos;
    std::optional<size_t> substream_pos;
    std::optional<size_t> compressed_pos;
    std::optional<size_t> uncompressed_pos;
    std::optional<size_t> codec_counts_pos;

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
