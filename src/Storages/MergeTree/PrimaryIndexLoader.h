#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <functional>

namespace DB
{

class IDataPartStorage;
struct KeyDescription;
struct ReadSettings;

/// Returns `primary.cidx` when the part stores its primary index compressed, `primary.idx` otherwise.
String getPrimaryIndexFileName(const IDataPartStorage & data_part_storage);

/// Drops the key columns after the first prefix that changes between at least `ratio_to_drop_suffix_columns` of the
/// adjacent marks. Returns false without looking at the columns when there are no marks, one key column, or the
/// ratio is outside (0, 1).
template <typename Columns>
bool dropPrimaryIndexSuffixColumns(size_t marks_count, Columns & index_columns, Float64 ratio_to_drop_suffix_columns);

/// Reads `marks_count` rows of the primary index from the part storage alone, so it works without an `IMergeTreeDataPart`.
/// `optimize_columns` runs on the read columns before their memory is counted, so it can drop columns.
/// The caller decides which memory tracker and arena the columns are charged to.
Columns loadPrimaryIndexFromStorage(
    const IDataPartStorage & data_part_storage,
    const KeyDescription & primary_key,
    size_t marks_count,
    const ReadSettings & read_settings,
    const std::function<void(MutableColumns &)> & optimize_columns);

}
