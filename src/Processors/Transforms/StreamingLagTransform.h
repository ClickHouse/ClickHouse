#pragma once
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Common/HashTable/Hash.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <base/types.h>
#include <Processors/ISimpleTransform.h>
#include <optional>
#include <unordered_map>

namespace DB
{

/// Computes `lagInFrame` window functions in a single streaming pass over data
/// that is already sorted by `(prefix_columns..., order_by_columns...)` (storage
/// ordering).  Instead of the full sort + `WindowTransform` pipeline it keeps a
/// small hash map — one entry per distinct suffix-partition key per prefix group —
/// so peak memory is O(distinct partitions × prefix group) rather than O(all rows).
///
/// Applicable only when:
///   - Every window function is `lagInFrame` with offset 1 (the default), with an
///     optional explicit default value that is a constant.
///   - The window PARTITION BY begins with the storage ORDER BY prefix columns
///     (`prefix_description`); the remaining PARTITION BY columns are the suffix.
///   - The window ORDER BY equals the remaining storage ORDER BY columns (so data
///     within each prefix group arrives in the correct window order).
///
/// State lifecycle:
///   - When the prefix key changes, the entire hash map is cleared.
///   - For each row: look up the suffix partition key in the map, emit the stored
///     previous value (or the configured default for the first occurrence), then
///     update the map.
class StreamingLagTransform : public ISimpleTransform
{
public:
    StreamingLagTransform(
        const SharedHeader & input_header_,
        SharedHeader output_header_,
        SortDescription prefix_description_,
        std::vector<std::string> suffix_partition_col_names_,
        std::vector<std::string> value_col_names_,
        std::vector<std::string> output_col_names_,
        std::vector<std::optional<Field>> default_values_);

    String getName() const override { return "StreamingLag"; }

protected:
    void transform(Chunk & chunk) override;

private:
    SortDescription prefix_description_;

    std::vector<size_t> prefix_col_indices_;
    std::vector<size_t> suffix_col_indices_;
    std::vector<size_t> value_col_indices_;

    std::vector<DataTypePtr> result_types_;
    std::vector<std::string> result_names_;
    std::vector<std::optional<Field>> default_values_;

    /// SipHash128 of the last seen prefix key columns — stable across chunk boundaries.
    UInt128 current_prefix_hash_{0, 0};
    bool first_row_ = true;

    /// SipHash128(suffix columns) → last value per function.
    /// 128-bit hash key avoids per-row string allocation; collision probability < 2^{-127}.
    std::unordered_map<UInt128, std::vector<Field>, UInt128Hash> state_map_;

    /// Pre-cast view of a Map(LowCardinality(String), String) column for zero-virtual-dispatch
    /// per-row hashing.  Built once per chunk in transform().
    struct MapColView
    {
        const ColumnArray::Offsets * array_offsets;   ///< Map's array extent per row
        const ColumnString::Chars * key_chars;         ///< dictionary char data
        const ColumnString::Offsets * key_str_offsets; ///< dictionary string offsets
        const ColumnString::Chars * val_chars;         ///< value char data
        const ColumnString::Offsets * val_str_offsets; ///< value string offsets
        const UInt8 * idx_u8 = nullptr;
        const UInt16 * idx_u16 = nullptr;
        const UInt32 * idx_u32 = nullptr;
    };

    /// Try to build a MapColView for column at position `col_idx` in `block`.
    /// Returns nullopt if the column is not Map(LowCardinality(String), String).
    static std::optional<MapColView> tryGetMapView(const Block & block, size_t col_idx);

    /// Hash one row of a pre-cast Map column into `hash` with no virtual dispatch.
    static void hashMapRow(const MapColView & view, size_t row, SipHash & hash);
};

}
