#pragma once
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

namespace DB
{

/// Reader for compact parts, that uses one buffer for
/// all column and doesn't support parallel prefetch of columns.
/// It's suitable for compact parts with small size of stripe.
class MergeTreeReaderCompactSingleBuffer : public MergeTreeReaderCompact
{
public:
    template <typename... Args>
    explicit MergeTreeReaderCompactSingleBuffer(Args &&... args)
        : MergeTreeReaderCompact{std::forward<Args>(args)...}
    {
        fillColumnPositions();
    }

    /// Returns the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, size_t current_range_last_mark,
                    bool continue_reading, size_t max_rows_to_read,
                    MutableColumns & res_columns, Columns & shared_columns) override;

    void updateReadRequestMap(MarkRangesPtr request_map) override
    {
        MergeTreeReaderCompact::updateReadRequestMap(request_map);
        if (stream)
            stream->updateReadRequestMap(std::move(request_map));
    }

private:
    MergeTreeReaderStream & getStream(const NameAndTypePair &) override { return *stream; }
    void readGranule(size_t mark, size_t rows_to_read, MutableColumns & res_columns, const std::vector<bool> * columns_mask) override;
    void init();

    bool initialized = false;
    std::unique_ptr<MergeTreeReaderStream> stream;

    /// Use cache to avoid reading the column with the same name twice.
    /// It may happen if there are empty array Nested in the part.
    GranuleColumnsCache granule_columns_cache;
    absl::flat_hash_map<String, ISerialization::SubstreamsDeserializeStatesCache> granule_deserialize_states_caches;

    /// If we don't have substream marks and we need to read multiple subcolumns from a single column in storage,
    /// we will read the whole column only once and then reuse to extract all subcolumns.
    /// We cannot use SubstreamsCache for it, because we may also read the full column itself
    /// and it might be not empty inside res_columns (and SubstreamsCache contains the whole columns).
    GranuleColumnsCache granule_columns_cache_for_subcolumns;
};

}
