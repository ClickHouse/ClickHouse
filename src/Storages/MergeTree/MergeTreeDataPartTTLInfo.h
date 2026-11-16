#pragma once

#include <base/types.h>

#include <map>
#include <optional>
#include <vector>
#include <ctime>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct TTLDescription;
using TTLDescriptions = std::vector<TTLDescription>;

/// Minimal and maximal ttl for column or table
struct MergeTreeDataPartTTLInfo
{
    time_t min = 0;
    time_t max = 0;

    /// This TTL was computed on completely expired part. It doesn't make sense
    /// to select such parts for TTL again. But make sense to recalculate TTL
    /// again for merge with multiple parts.
    std::optional<bool> ttl_finished;
    bool finished() const { return ttl_finished.value_or(false); }
    bool initialized() const { return min != 0 || max != 0; }

    void update(time_t time);
    void update(const MergeTreeDataPartTTLInfo & other_info);
};

/// Order is important as it would be serialized and hashed for checksums
using TTLInfoMap = std::map<String, MergeTreeDataPartTTLInfo>;

/// PartTTLInfo for all columns and table with minimal ttl for whole part
struct MergeTreeDataPartTTLInfos
{
    TTLInfoMap columns_ttl;
    MergeTreeDataPartTTLInfo table_ttl;

    /// `part_min_ttl` and `part_max_ttl` are TTLs which are used for selecting parts
    /// to merge in order to remove expired rows.
    time_t part_min_ttl = 0;
    time_t part_max_ttl = 0;

    TTLInfoMap rows_where_ttl;

    TTLInfoMap moves_ttl;

    TTLInfoMap recompression_ttl;

    TTLInfoMap group_by_ttl;

    /// The `time_of_merge` of the latest `TTLDelete` (`TTLRecompress`) merge among the merges that produced
    /// the part or any of the parts it was made of, zero if there was none. Set only with `persist_ttl_merge_times`.
    /// The merge selector postpones the next TTL merge of a partition by `merge_with_ttl_timeout`
    /// (`merge_with_recompression_ttl_timeout`) from it, so the postponement survives a restart of the server and
    /// covers the merges assigned by the other replicas. These are not TTL bounds: the algorithms that rebuild the
    /// bounds keep them, see `resetBounds`.
    time_t last_delete_ttl_merge_time = 0;
    time_t last_recompression_ttl_merge_time = 0;

    /// Return the smallest max recompression TTL value
    time_t getMinimalMaxRecompressionTTL() const;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
    void update(const MergeTreeDataPartTTLInfos & other_infos);

    /// Drop all TTL bounds before they are calculated again, but keep the times of the past TTL merges.
    void resetBounds();

    /// Has any TTLs which are not calculated on completely expired parts.
    bool hasAnyNonFinishedTTLs() const;

    /// Has any row TTL (table, `WHERE` or `GROUP BY`) which is not calculated on a completely expired part.
    bool hasAnyNonFinishedRowTTLs() const;

    /// Has any column TTL which is not calculated on a completely expired part. A column TTL can only
    /// be honoured by rewriting the part, never by dropping it, so it is tracked separately.
    bool hasAnyNonFinishedColumnTTLs() const;

    /// The earliest time at which a column TTL becomes due. Zero if there is no unfinished column TTL.
    time_t getMinimalNonFinishedColumnTTL() const;

    void updatePartMinMaxTTL(const MergeTreeDataPartTTLInfo & ttl_info)
    {
        if (ttl_info.finished())
            return;

        if (ttl_info.min && (!part_min_ttl || ttl_info.min < part_min_ttl))
            part_min_ttl = ttl_info.min;

        if (ttl_info.max && (!part_max_ttl || ttl_info.max > part_max_ttl))
            part_max_ttl = ttl_info.max;
    }

    bool empty() const
    {
        /// part_min_ttl in minimum of rows, rows_where and group_by TTLs
        return !part_min_ttl && moves_ttl.empty() && recompression_ttl.empty() && columns_ttl.empty() && rows_where_ttl.empty() && group_by_ttl.empty();
    }
};

/// Selects the most appropriate TTLDescription using TTL info and current time.
std::optional<TTLDescription> selectTTLDescriptionForTTLInfos(const TTLDescriptions & descriptions, const TTLInfoMap & ttl_info_map, time_t current_time, bool use_max);

/// True if a `RECOMPRESS` TTL entry from `recompression_ttl_entries` is due at `current_time` and its codec is not `Default` (=is explicit).
bool isExplicitRecompression(
    const TTLDescriptions & recompression_ttl_entries, const TTLInfoMap & recompression_ttl_info, time_t current_time);

}
