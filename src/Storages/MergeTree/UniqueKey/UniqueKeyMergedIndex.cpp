#include <Storages/MergeTree/UniqueKey/UniqueKeyMergedIndex.h>

#include "config.h"

#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/UniqueKey/ReadSnapshot.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Common/Exception.h>

#if USE_ROCKSDB
#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>
#include <rocksdb/table_properties.h>
#endif

#include <limits>
#include <queue>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int ROCKSDB_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

UniqueKeyMergeRowMap::UniqueKeyMergeRowMap(
    const MergeTreeDataPartsVector & sources,
    const ReadSnapshot & read_snapshot,
    const MergedPartOffsets & merged_part_offsets_)
    : merged_part_offsets(merged_part_offsets_)
    , merged_start(sources.size())
{
    chassert(merged_part_offsets.isFinalized());

    source_bitmaps.reserve(sources.size());
    for (size_t i = 0; i < sources.size(); ++i)
    {
        source_bitmaps.push_back(read_snapshot.bitmapAt(sources[i]->info));
        const UInt64 live_at_snapshot = sources[i]->rows_count - source_bitmaps[i]->cardinality();
        if (merged_part_offsets.isMappingEnabled() && merged_part_offsets.getPartRowsCount(i) != live_at_snapshot)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "UNIQUE KEY merge: mapped {} row(s) of source part {}, which had {} live at the snapshot",
                merged_part_offsets.getPartRowsCount(i), sources[i]->name, live_at_snapshot);

        merged_start[i] = merged_rows_count;
        merged_rows_count += live_at_snapshot;
    }
}

/// The merge skipped the rows dead at the snapshot, so its row mapping indexes the rows each source
/// sent: source row `r` is the `r - rank(r)`-th, where `rank(r)` counts the snapshot bitmap's rows
/// before `r`. Without a sorting key the merge appends the sources in order.
///
/// TODO(unique-key): `rangeCardinality` walks the bitmap's containers before `source_row` for every
/// index entry. A per-source array of merged rows, filled in one pass over the bitmap, would make
/// each lookup one read.
std::optional<UInt64> UniqueKeyMergeRowMap::toMergedRow(size_t source_index, UInt64 source_row) const
{
    const DeleteBitmap & source_bitmap = *source_bitmaps[source_index];
    if (source_bitmap.contains(source_row))
        return std::nullopt;

    const UInt64 live_index = source_row - source_bitmap.rangeCardinality(0, source_row);
    if (merged_part_offsets.isMappingEnabled())
        return merged_part_offsets[source_index, live_index];
    return merged_start[source_index] + live_index;
}

UniqueKeyMergedIndexBuilder::UniqueKeyMergedIndexBuilder(
    const MergeTreeDataPartsVector & sources_,
    const UniqueKeyMergeRowMap & row_map_,
    IDataPartStorage & merged_part_storage_,
    ContextPtr context_)
    : sources(sources_)
    , row_map(row_map_)
    , merged_part_storage(merged_part_storage_)
    , context(std::move(context_))
{
}

#if USE_ROCKSDB
namespace
{
    /// One source's index, read in key order.
    struct SourceCursor
    {
        size_t source_index;
        const IMergeTreeDataPart * part;
        SSTFileReaderPtr reader;
        /// After `reader`, so it is destroyed first.
        std::unique_ptr<rocksdb::Iterator> iterator;

        /// False once the source is exhausted; a read error throws instead of ending the scan early.
        bool hasEntry() const
        {
            if (iterator->Valid())
                return true;
            const auto status = iterator->status();
            if (status.ok())
                return false;
            throw Exception(status.IsCorruption() ? ErrorCodes::CORRUPTED_DATA : ErrorCodes::ROCKSDB_ERROR,
                "Failed to read the dense index of source part {}: {}", part->name, status.ToString());
        }
    };

    /// Each non-empty source positioned at its first entry; its entry count is checked first, so a
    /// stale index fails before anything is written.
    std::vector<SourceCursor> openSourceCursors(const MergeTreeDataPartsVector & sources, const ReadSettings & read_settings)
    {
        std::vector<SourceCursor> cursors;
        for (size_t i = 0; i < sources.size(); ++i)
        {
            const IMergeTreeDataPart & part = *sources[i];
            if (part.rows_count == 0)
                continue;
            if (!part.checksums.has(SSTIndexWriter::FILE_NAME))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "UNIQUE KEY source part {} has {} rows but no dense index", part.name, part.rows_count);

            auto reader = openSSTReaderFromStorage(part.getDataPartStoragePtr(), SSTIndexWriter::FILE_NAME, read_settings);
            const auto properties = reader->getProperties();
            const UInt64 entries = properties ? properties->num_entries : 0;
            if (entries != part.rows_count)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Dense index of source part {} has {} entries, but the part has {} rows",
                    part.name, entries, part.rows_count);

            SourceCursor cursor{.source_index = i, .part = &part, .reader = reader, .iterator = reader->newIterator()};
            cursor.iterator->SeekToFirst();
            if (cursor.hasEntry())
                cursors.push_back(std::move(cursor));
        }
        return cursors;
    }
}
#endif

void UniqueKeyMergedIndexBuilder::build(MergeTreeDataPartChecksums & out_checksums, bool fsync)
{
#if USE_ROCKSDB
    const UInt64 merged_rows_count = row_map.mergedRowsCount();
    if (merged_rows_count > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "UNIQUE KEY merge: merged part has {} rows, exceeds the dense index's UInt32 row-number capacity",
            merged_rows_count);

    std::vector<SourceCursor> cursors = openSourceCursors(sources, context->getReadSettings());

    /// Min-heap on each source's current key: entries leave in ascending order across the sources,
    /// as `SSTIndexWriter::addEncoded` requires.
    auto key_greater = [&](size_t lhs, size_t rhs)
    {
        return cursors[lhs].iterator->key().compare(cursors[rhs].iterator->key()) > 0;
    };
    std::priority_queue<size_t, std::vector<size_t>, decltype(key_greater)> heap(key_greater);
    for (size_t c = 0; c < cursors.size(); ++c)
        heap.push(c);

    SSTIndexWriter writer(merged_part_storage, context);
    while (!heap.empty())
    {
        const size_t c = heap.top();
        heap.pop();
        SourceCursor & cursor = cursors[c];

        const rocksdb::Slice value = cursor.iterator->value();
        const UInt64 source_row = decodeRowNumberBE(value.data(), value.size());
        if (source_row >= cursor.part->rows_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Dense index of source part {} points at row {}, but the part has {} rows",
                cursor.part->name, source_row, cursor.part->rows_count);

        if (const auto merged_row = row_map.toMergedRow(cursor.source_index, source_row))
        {
            const rocksdb::Slice key = cursor.iterator->key();
            writer.addEncoded(std::string_view(key.data(), key.size()), static_cast<UInt32>(*merged_row));
        }

        cursor.iterator->Next();
        if (cursor.hasEntry())
            heap.push(c);
    }

    if (writer.entriesAdded() != merged_rows_count)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "UNIQUE KEY merge: the sources' dense indexes hold {} entries live at the snapshot, but the merged part has {} rows",
            writer.entriesAdded(), merged_rows_count);

    writer.finish(out_checksums, fsync);
#else
    /// The reference members are read only on the RocksDB path; without these `-Wunused-private-field` fails the build.
    (void)sources; (void)row_map; (void)merged_part_storage; (void)out_checksums; (void)fsync;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UNIQUE KEY merge requires RocksDB support (USE_ROCKSDB=1)");
#endif
}

}
