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

    /// Whether `update` saw a timestamp of exactly 0 (the epoch). The TTL machinery treats such a
    /// timestamp as "no TTL": `ITTLAlgorithm::isTTLExpired` never expires it and `update` excludes it
    /// from `min`, so a row that computed to it is invisible in the stored bounds. Transient - used
    /// while the infos are being computed to decide whether the rows-TTL fingerprint may be recorded
    /// (see `MergeTreeDataPartTTLInfos::table_ttl_expression`); not serialized to `ttl.txt`.
    bool has_epoch_timestamps = false;

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

    /// The rows-TTL (DELETE) expression under which `table_ttl` was computed, stored as its serialized
    /// result-column expression (matching the keys used in the other TTL maps). Empty means unknown:
    /// e.g. a part written by an older server, or a merge whose source parts disagree on the expression.
    /// The fast `MATERIALIZE TTL` optimization uses this to verify that a part's stored TTL timestamps really
    /// correspond to the expression it is shifting from; otherwise it falls back to a full rewrite.
    String table_ttl_expression;

    /// The time zone the rows-TTL timestamps of `table_ttl` were computed under, as returned by
    /// `getRowsTTLTimeZoneFingerprint`. Empty means unknown, exactly like `table_ttl_expression`.
    /// It is a separate part of the fingerprint because the expression text does not pin the time zone
    /// down: a `DateTime` column can change its zone with a metadata-only `MODIFY COLUMN`, and the
    /// server time zone can change with a restart.
    String table_ttl_timezone;

    /// `part_min_ttl` and `part_max_ttl` are TTLs which are used for selecting parts
    /// to merge in order to remove expired rows.
    time_t part_min_ttl = 0;
    time_t part_max_ttl = 0;

    TTLInfoMap rows_where_ttl;

    TTLInfoMap moves_ttl;

    TTLInfoMap recompression_ttl;

    TTLInfoMap group_by_ttl;

    /// Return the smallest max recompression TTL value
    time_t getMinimalMaxRecompressionTTL() const;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
    void update(const MergeTreeDataPartTTLInfos & other_infos);

    /// Has any TTLs which are not calculated on completely expired parts.
    bool hasAnyNonFinishedTTLs() const;

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
