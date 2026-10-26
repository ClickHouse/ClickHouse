#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class MergeTreeDataPartCompact;
using DataPartCompactPtr = std::shared_ptr<const MergeTreeDataPartCompact>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// Base class of readers for compact parts.
class MergeTreeReaderCompact : public IMergeTreeReader
{
public:
    MergeTreeReaderCompact(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeSettingsPtr & storage_settings_,
        UncompressedCache * uncompressed_cache_,
        ColumnsCache * columns_cache_,
        MarkCache * mark_cache_,
        DeserializationPrefixesCache * deserialization_prefixes_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_);

    bool canReadIncompleteGranules() const final { return false; }

    size_t getColumnsCacheStripeMarks() const override
    {
        return columns_cache_reads_possible || columns_cache_writes_possible ? stripes.stripe_marks : 0;
    }

    bool canReadWholeColumnsCacheStripe(size_t mark) override;

protected:
    /// Fills the positions of the columns in the part, and decides which of them can use the columns cache.
    void fillColumnPositions();

    /// Read the whole granules from `from_mark` on, at least `max_rows_to_read` rows of them,
    /// from the part or from the columns cache, see `IMergeTreeReader::readRows`.
    size_t readGranules(
        size_t from_mark, size_t current_range_last_mark, bool continue_reading, size_t max_rows_to_read,
        MutableColumns & res_columns, Columns & shared_columns);

    /// Read the granule `mark` of `rows_to_read` rows from the part, appending it to `res_columns`:
    /// the columns `columns_mask` selects, or all of them that are in the part if it is null.
    virtual void readGranule(size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask) = 0;

    /// The granules served from the columns cache, by the position of the column in `columns_to_read`, since
    /// `takeCompressedBytesRead` was called. Only with `countCompressedBytesRead`.
    std::vector<std::pair<size_t, size_t>> granules_from_columns_cache;
    /// The columns read from the part in the granules that also have columns served from the cache, by granule.
    std::unordered_map<size_t, std::vector<size_t>> columns_read_from_part_in_granules;

    /// The positions in the marks that the column at `pos` of `columns_to_read` is read from.
    std::vector<size_t> getPositionsInMarks(size_t pos) const;

    /// The compressed size of the data reading `granules_from_columns_cache` from the part would load. Resets them.
    size_t takeCompressedBytesOfColumnsCacheHits(MergeTreeReaderStream & stream);

    using InputStreamGetter = ISerialization::InputStreamGetter;

    void readData(
        size_t column_idx,
        IColumn & column,
        size_t rows_to_read,
        size_t from_mark,
        size_t column_size_before_reading,
        MergeTreeReaderStream & stream,
        std::unordered_map<String, ColumnPtr> & output_columns_cache,
        std::unordered_map<String, ColumnPtr> * columns_cache_for_subcolumns,
        ISerialization::SubstreamsCache * substreams_cache);

    virtual MergeTreeReaderStream & getStream(const NameAndTypePair & column) = 0;

    void readPrefix(size_t column_idx, size_t from_mark, MergeTreeReaderStream & stream, ISerialization::SubstreamsDeserializeStatesCache * cache);

    void readSubcolumnsPrefixes(size_t from_mark);
    void initSubcolumnsDeserializationOrder();

    void createColumnsForReading(MutableColumns & res_columns) const;
    bool needSkipStream(size_t column_pos, const ISerialization::SubstreamPath & substream) const;

    const ColumnsSubstreams & columns_substreams;

    MergeTreeMarksLoaderPtr marks_loader;
    MergeTreeMarksGetterPtr marks_getter;

    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;

    /// Storage columns with collected separate arrays of Nested to columns of Nested type.
    /// They maybe be needed for finding offsets of missed Nested columns in parts.
    /// They are rarely used and are heavy to initialized, so we create them
    /// only on demand and cache in this field.
    std::optional<ColumnsDescription> storage_columns_with_collected_nested;

    /// Positions of columns in part structure.
    using ColumnPositions = std::vector<std::optional<size_t>>;
    ColumnPositions column_positions;

    /// Should we read full column or only it's offsets.
    /// Element of the vector is the level of the alternative stream.
    std::vector<std::optional<ColumnForOffsets>> columns_for_offsets;

    /// Mark to read in next 'readRows' call in case,
    /// when 'continue_reading' is true.
    size_t next_mark = 0;

    /// True if we have marks per each substream and not per each column.
    bool has_substream_marks;
    bool has_subcolumns = false;
    /// Remember indexes of subcolumns in columns_to_read list for each column if any.
    std::unordered_map<String, std::vector<size_t>> column_to_subcolumns_indexes;
    /// If we have marks for substreams we read all subcolumns of the same column from a single stream
    /// by seeking to the required substream during deserialization. To avoid seeks back in the file
    /// we deserialize subcolumns of the same column in the order of their serialization.
    std::unordered_map<String, std::vector<size_t>> subcolumns_deserialization_order;

    DeserializationPrefixesCache * deserialization_prefixes_cache;
    DeserializeBinaryBulkStateMap cached_subcolumn_prefixes;

private:
    /// The columns cache.
    ///
    /// Every granule of every column of a compact part is serialized on its own, with its own
    /// prefix, and is read after a seek to its mark. So unlike in a wide part, there are no
    /// stream positions to keep: a read goes granule by granule and column by column, a column
    /// whose entry covers the granule is served from it and the others are read from the part.
    /// A block that is a whole entry is served from it, and written to it, without a copy: the
    /// result column and the entry are the same column, immutable from then on. The granules of
    /// other blocks are copied, out of an entry or into one, which is written once its run of
    /// granules ends - the reader reads only whole granules, so every granule it copies is complete.

    /// Whether the column at `pos` has entries in the cache: it is in the part, read in full (not
    /// only for the offsets of a `Nested` sibling) and its rows are a function of the granule.
    /// The columns that are in the part but not cacheable are read from the part for every granule.
    std::vector<bool> column_cacheable;
    bool has_columns_read_only_from_part = false;

    /// The contiguous mark range being read, [range_first_mark, range_end_mark), the entries of
    /// it per result column and per stripe of the range (nullptr when the stripe has no entry).
    size_t range_first_mark = 0;
    size_t range_end_mark = 0;
    size_t range_first_stripe = 0;
    std::vector<std::vector<ColumnsCache::MappedPtr>> cached_entries;

    /// Per result column, the run of granules being copied for the cache: consecutive granules of
    /// one stripe read from the part, from `first_mark` to `next_mark`.
    struct Accumulation
    {
        size_t stripe = 0;
        size_t first_mark = 0;
        size_t next_mark = 0;
        MutableColumnPtr column;
    };
    std::vector<Accumulation> accumulations;

    /// Begin a new contiguous mark range at `from_mark`: write what the previous range left,
    /// and look the stripes of the new one up in the cache.
    void startColumnsCacheRange(size_t from_mark, size_t end_mark);

    /// Look the stripes of the granules [first_mark, end_mark) up in the cache for every cacheable column.
    std::vector<std::vector<ColumnsCache::MappedPtr>> lookupColumnsCache(size_t first_mark, size_t end_mark) const;

    const ColumnsCache::MappedPtr & cachedEntryFor(size_t pos, size_t mark) const;
    bool isGranuleColumnCached(size_t pos, size_t mark) const;

    size_t readGranulesWithColumnsCache(size_t from_mark, size_t max_rows_to_read, MutableColumns & res_columns, Columns & shared_columns);

    /// `serveRowsFromColumnsCacheEntry` for the granules [first_mark, end_mark), which also remembers them, see
    /// `granules_from_columns_cache`.
    void serveRowsFromColumnsCache(
        const ColumnsCacheEntry & entry, size_t pos, size_t first_mark, size_t end_mark, MutableColumns & res_columns, Columns & shared_columns);

    /// Copy the `rows` rows of granule `mark` of the column at `pos`, which start at `offset` of
    /// `column`, for the cache.
    void accumulateGranuleForColumnsCache(size_t pos, size_t mark, const IColumn & column, size_t offset, size_t rows);

    /// Turn the accumulated run of granules of the column at `pos` into an entry.
    void finishAccumulation(size_t pos);
    void finishAllAccumulations();

    void readPrefix(
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        ISerialization::DeserializeBinaryBulkStatePtr & state,
        const InputStreamGetter & buffer_getter,
        ISerialization::SubstreamsDeserializeStatesCache * cache,
        ISerialization::DeserializeBinaryBulkSettings::CheckStreamExistsCallback check_stream_exists_callback = {});

    NameAndTypePair getColumnConvertedToSubcolumnOfNested(const NameAndTypePair & column);
    void findPositionForMissedNested(size_t pos);
};

}
