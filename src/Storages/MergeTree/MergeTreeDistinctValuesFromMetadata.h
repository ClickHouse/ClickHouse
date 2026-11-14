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
/// - the columns of the primary key, from the values of the marks of the primary key index;
/// - physical columns of scalar types with general purpose codecs whose compressed size is known in at least one
///   part (compact parts do not keep the sizes of their columns), from the compressed size.
std::unordered_map<String, UInt64> estimateDistinctValuesFromMetadata(
    const RangesInDataParts & parts, const StorageMetadataPtr & metadata, const Names & column_names);

}
