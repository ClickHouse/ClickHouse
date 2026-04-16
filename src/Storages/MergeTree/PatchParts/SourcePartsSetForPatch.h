#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Core/Block.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** A helper index of source parts for which updated data is stored in the patch part.
  * It is used to get patches for the regular parts.
  */
class SourcePartsSetForPatch
{
public:
    /// On-disk format version of the patch part.
    ///  0 = legacy v1 (sorted by `_part, _part_offset`, applied with `Merge` or `Join` mode).
    ///  1 = v2 (sorted by `sort_key..., _block_number, _block_offset`, applied with `MergeOnKey`).
    static constexpr UInt8 V1_FORMAT_VERSION = 0;
    static constexpr UInt8 V2_FORMAT_VERSION = 1;
    static constexpr UInt8 MAX_SUPPORTED_FORMAT_VERSION = V2_FORMAT_VERSION;

    static constexpr auto FILENAME = "source_parts.dat";

    SourcePartsSetForPatch() = default;

    bool empty() const { return min_max_versions_by_part.empty(); }
    UInt64 getMinDataVersion() const { return min_data_version; }
    UInt64 getMaxDataVersion() const { return max_data_version; }

    UInt64 getMinDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).first; }
    UInt64 getMaxDataVersion(const String & part_name) const { return min_max_versions_by_part.at(part_name).second; }

    UInt8 getFormatVersion() const { return format_version; }
    void setFormatVersion(UInt8 new_format_version) { format_version = new_format_version; }

    /// Sort-key **expression list** of the target table captured at patch-write time, serialized
    /// as SQL so it round-trips through v2's on-disk `source_parts.dat` and can be replayed at
    /// read time to materialize sort-key columns from physical source columns — the same trick
    /// FINAL uses. Plus the parallel DESC-flag vector. Empty for v1 patches.
    ///
    /// For plain sort keys (e.g. `ORDER BY id`) this SQL is just `id`; for expression sort keys
    /// (e.g. `ORDER BY cityHash64(id)`) it is `cityHash64(id)`. The patch part on disk stores the
    /// expression's **input (source) columns** (`id`), and the expression is replayed on both
    /// sides at apply time to produce the sort-key column the two-cursor merge compares on.
    const String & getSortKeyExprListSQL() const { return sort_key_expr_list_sql; }
    const std::vector<UInt8> & getSortKeyReverseFlags() const { return sort_key_reverse_flags; }
    void setSortKey(String expr_list_sql, std::vector<UInt8> reverse_flags);

    void addSourcePart(const String & name, UInt64 data_version);
    PatchParts getPatchParts(const MergeTreePartInfo & original_part, const DataPartPtr & patch_part) const;

    static SourcePartsSetForPatch build(const Block & block, UInt64 data_version);
    static SourcePartsSetForPatch merge(const DataPartsVector & source_parts);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

private:
    void buildSourcePartsSet();

    /// Max data version -> part set that contains all parts from min_max_versions_by_part with this max data version.
    /// Can be reconstructed from source_parts_by_version.
    std::map<UInt64, ActiveDataPartSet> source_parts_by_version;

    /// Part name -> min and max version of updated data stored in patch part for the source part.
    /// Serialized to the file on disk.
    std::map<String, std::pair<UInt64, UInt64>> min_max_versions_by_part;

    UInt64 min_data_version = 0;
    UInt64 max_data_version = 0;

    /// Format version of the patch part on disk. Populated on read from the version byte;
    /// set explicitly by the sink before write. See `V1_FORMAT_VERSION` / `V2_FORMAT_VERSION`.
    UInt8 format_version = V1_FORMAT_VERSION;

    /// v2-only. SQL of the target table's sort-key expression list captured at write time —
    /// e.g. `cityHash64(id)` for `ORDER BY cityHash64(id)`, or `a, b` for `ORDER BY (a, b)`.
    /// Persisted so readers can rebuild the patch's sort key including the **expression** (the
    /// ExpressionActions is re-derived by `KeyDescription::getKeyFromAST` on load) even if the
    /// main table's sort key was later changed via ALTER.
    String sort_key_expr_list_sql;
    /// Parallel to the top-level children of `sort_key_expr_list_sql`. 1 if DESC, else 0.
    std::vector<UInt8> sort_key_reverse_flags;
};

/// Returns set with source parts with _part column from block and data_version.
/// Updates _data_version in block with const value (data_version).
SourcePartsSetForPatch buildSourceSetForPatch(Block & block, UInt64 data_version);

}
