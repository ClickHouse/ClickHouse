#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

namespace DB
{

class IDataPartStorage;
struct KeyDescription;
struct MergeTreeSettings;
struct ReadSettings;

/// Returns `primary.cidx` when the part stores its primary index compressed, `primary.idx` otherwise.
String getPrimaryIndexFileName(const IDataPartStorage & data_part_storage);

/// Drops the key columns after the first prefix that changes between at least `ratio_to_drop_suffix_columns` of the
/// adjacent marks. No-op when the ratio is outside (0, 1).
template <typename Columns>
void dropPrimaryIndexSuffixColumns(size_t marks_count, Columns & index_columns, Float64 ratio_to_drop_suffix_columns);

/// Reads `marks_count` rows of the primary index from the part storage alone, so it works without an `IMergeTreeDataPart`.
/// The caller decides which memory tracker and arena the columns are charged to.
Columns loadPrimaryIndexFromStorage(
    const IDataPartStorage & data_part_storage,
    const KeyDescription & primary_key,
    size_t marks_count,
    const MergeTreeSettings & storage_settings,
    const ReadSettings & read_settings,
    bool keep_all_columns);

}
