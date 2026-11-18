#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <unordered_map>

namespace DB
{

/// Estimates the numbers of distinct values of `column_names` in `parts` from the metadata of the parts that is in
/// memory, without reading data, see the setting `estimate_distinct_values_from_metadata`. Returns the columns
/// with an estimate:
/// - the first column of the primary key, from the values of the marks of the primary key index that is in memory;
/// - physical columns of scalar types outside of the sorting key with general purpose codecs whose compressed size is
///   known in at least one part (compact parts do not keep the sizes of their columns), from the compressed size. A
///   column compressed by the default codec of the parts gets an estimate only when that codec is a general purpose
///   one in every wide part and the table does not select codecs adaptively (`enable_adaptive_codec_selection`).
std::unordered_map<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names);

}
