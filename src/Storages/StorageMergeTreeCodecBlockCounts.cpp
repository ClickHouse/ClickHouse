#include <Storages/StorageMergeTreeCodecBlockCounts.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/IColumn.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/getCompressionCodecForFile.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/ISerialization.h>
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
const String CODEC_BLOCK_COUNTS_COLUMN = "codec_block_counts";
const String SUBCOLUMNS_NAMES_COLUMN = "subcolumns.names";
const String SUBCOLUMNS_CODEC_BLOCK_COUNTS_COLUMN = "subcolumns.codec_block_counts";

Map codecCountsToField(const std::map<String, UInt64> & counts)
{
    Map result;
    result.reserve(counts.size());
    for (const auto & [name, count] : counts)
        result.emplace_back(Tuple({name, count}));
    return result;
}

/// Per-(part, column) result: the maps come from walking the column's `.bin` headers, `subcolumn_names` from metadata.
struct ColumnCodecInfo
{
    Map column_map;
    Array subcolumn_names;
    Array subcolumn_maps;
};

class MergeTreeCodecBlockCountsSource final : public ISource
{
public:
    MergeTreeCodecBlockCountsSource(
        SharedHeader header_,
        MergeTreeData::DataPartsVector data_parts_,
        MergeTreeSettingsPtr storage_settings_,
        ReadSettings read_settings_)
        : ISource(header_)
        , header(std::move(header_))
        , data_parts(std::move(data_parts_))
        , storage_settings(std::move(storage_settings_))
        , read_settings(std::move(read_settings_))
    {
        need_column_map = header->has(CODEC_BLOCK_COUNTS_COLUMN);
        need_subcolumn_maps = header->has(SUBCOLUMNS_CODEC_BLOCK_COUNTS_COLUMN);
        need_subcolumn_names = header->has(SUBCOLUMNS_NAMES_COLUMN);
    }

    String getName() const override { return "MergeTreeCodecBlockCounts"; }

protected:
    Chunk generate() override
    {
        /// Each call processes the next part and returns one row for each of its columns.
        if (part_index >= data_parts.size())
            return {};

        const auto & part = data_parts[part_index++];
        const auto part_columns = part->getColumns();
        const size_t num_rows = part_columns.size();

        MutableColumns result(header->columns());
        for (size_t pos = 0; pos < header->columns(); ++pos)
            result[pos] = header->getByPosition(pos).type->createColumn();

        size_t column_position = 0;
        for (const auto & column : part_columns)
        {
            ++column_position;
            ColumnCodecInfo info = computeForColumn(part, column, column_position);

            for (size_t pos = 0; pos < header->columns(); ++pos)
            {
                const auto & name = header->getByPosition(pos).name;
                if (name == PART_NAME_COLUMN)
                    result[pos]->insert(part->name);
                else if (name == COLUMN_COLUMN)
                    result[pos]->insert(column.name);
                else if (name == CODEC_BLOCK_COUNTS_COLUMN)
                    result[pos]->insert(info.column_map);
                else if (name == SUBCOLUMNS_NAMES_COLUMN)
                    result[pos]->insert(info.subcolumn_names);
                else if (name == SUBCOLUMNS_CODEC_BLOCK_COUNTS_COLUMN)
                    result[pos]->insert(info.subcolumn_maps);
                else
                    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "No such column {}", name);
            }
        }

        return Chunk(std::move(result), num_rows);
    }

private:
    /// Walking `.bin` headers reads data files, so it runs only when a counts column is selected.
    bool needWalk() const { return need_column_map || need_subcolumn_maps; }
    bool needSubcolumns() const { return need_subcolumn_names || need_subcolumn_maps; }

    ColumnCodecInfo computeForColumn(const MergeTreeDataPartPtr & part, const NameAndTypePair & column, size_t column_position)
    {
        ColumnCodecInfo info;
        auto serialization = part->getSerialization(column.name);

        std::map<String, std::map<String, UInt64>> per_substream_codec_counts;
        std::map<String, UInt64> column_codec_counts;

        /// Never walk Compact parts as their columns share one `data.bin` (per-column attribution is not derivable via header).
        if (needWalk() && !isCompactPart(part))
        {
            /// Gather the column's [sub]stream `.bin` file names.
            Strings filenames;
            if (column.type->hasDynamicSubcolumns() && !part->getColumnsSubstreams().empty())
            {
                const auto & column_substreams = part->getColumnsSubstreams().getColumnSubstreams(column_position - 1);
                for (const auto & substream : column_substreams)
                {
                    auto filename = IMergeTreeDataPart::getStreamNameOrHash(substream, ".bin", part->checksums);
                    filenames.push_back(filename.value_or(""));
                }
            }
            else
            {
                serialization->enumerateStreams(
                    [&](const auto & subpath)
                    {
                        auto filename
                            = IMergeTreeDataPart::getStreamNameForColumn(column.name, subpath, ".bin", part->checksums, storage_settings);
                        filenames.push_back(filename.value_or(""));
                    });
            }

            /// Walk each substream's blocks and count the codec of every block.
            for (const auto & fname : filenames)
            {
                if (fname.empty())
                    continue;

                const String bin_path = fname + ".bin";
                if (!part->checksums.files.contains(bin_path))
                    continue;

                auto read_buffer = part->getDataPartStorage().readFile(bin_path, read_settings, std::nullopt);
                auto & substream_counts = per_substream_codec_counts[fname];
                while (!read_buffer->eof())
                {
                    UInt32 size_compressed = 0;
                    UInt32 size_decompressed = 0;
                    auto codec = getCompressionCodecForFile(*read_buffer, size_compressed, size_decompressed, true);
                    const auto codec_name = codec->getCodecDesc()->formatForLogging();
                    ++substream_counts[codec_name];
                    ++column_codec_counts[codec_name];
                }
            }
        }

        if (need_column_map)
            info.column_map = codecCountsToField(column_codec_counts);

        if (needSubcolumns())
        {
            IDataType::forEachSubcolumn(
                [&](const auto & subpath, const auto & name, const auto & data)
                {
                    /// Keep only file-backed subcolumns; no `.bin` means an intermediate composite or ephemeral subcolumn.
                    NameAndTypePair subcolumn(column.name, name, column.type, data.type);
                    auto stream_name
                        = IMergeTreeDataPart::getStreamNameForColumn(subcolumn, subpath, ".bin", part->checksums, storage_settings);
                    if (!stream_name)
                        return;

                    info.subcolumn_names.push_back(name);

                    if (need_subcolumn_maps)
                    {
                        Map field;
                        if (auto it = per_substream_codec_counts.find(*stream_name); it != per_substream_codec_counts.end())
                            field = codecCountsToField(it->second);
                        info.subcolumn_maps.emplace_back(std::move(field));
                    }
                },
                ISerialization::SubstreamData(serialization).withType(column.type));
        }

        return info;
    }

    SharedHeader header;
    MergeTreeData::DataPartsVector data_parts;
    MergeTreeSettingsPtr storage_settings;
    ReadSettings read_settings;

    bool need_column_map = false;
    bool need_subcolumn_maps = false;
    bool need_subcolumn_names = false;

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

    storage_settings = merge_tree->getSettings();
    data_parts = merge_tree->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

/// TODO: no filter pushdown. A `part_name`/`column` predicate still walks every part. Prune via a QueryPlan step.
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
    return Pipe(
        std::make_shared<MergeTreeCodecBlockCountsSource>(
            std::move(sample_block), data_parts, storage_settings, context->getReadSettings()));
}

}
