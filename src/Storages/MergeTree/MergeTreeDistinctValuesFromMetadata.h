#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/UnorderedMapWithMemoryTracking.h>

namespace DB
{

/// Estimates the numbers of distinct values of `column_names` in `parts` from the metadata of the parts, see the
/// setting `estimate_distinct_values_from_metadata`. Returns the columns with an estimate: physical columns of scalar
/// types outside of the sorting key with general purpose codecs whose compressed size is known in at least one part
/// (compact parts do not keep the sizes of their columns). A column compressed by the default codec of the parts gets
/// an estimate only when that codec is a general purpose one in every wide part and the table does not select codecs
/// adaptively (`enable_adaptive_codec_selection`).
///
/// The declared codec of a column is the current one, and a codec-only `ALTER` does not rewrite the parts. So every
/// wide part with the column also has to store it by general purpose codecs: on a local disk by the first compressed
/// frame of its data files, read once for a part (`IMergeTreeDataPart::isColumnStoredWithGeneralPurposeCodecs`); on a
/// remote disk, where that would be a request per file, a part of a replicated table has to be written with the
/// current metadata version instead, and a part of another table is taken as declared. No other data is read.
UnorderedMapWithMemoryTracking<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names);

}
