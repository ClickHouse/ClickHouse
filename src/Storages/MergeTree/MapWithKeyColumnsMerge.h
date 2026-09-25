#pragma once

#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <unordered_map>

namespace DB
{

class IDataType;
class ColumnMap;
struct MergeTreeSettings;

/// True when this part stores `column_name` with `with_key_columns` serialization.
bool partUsesMapWithKeyColumns(const IMergeTreeDataPart & part, const String & column_name);

/// True when the table setting will write merged parts with `with_key_columns`.
bool mergeOutputUsesMapWithKeyColumns(const MergeTreeSettings & settings, const IDataType & type);

/// Read `keys_info` from a Wide part. Compact parts should use `collectManifestFromColumn`
/// after reading the (small) Map column.
MapKeyManifest readMapKeyManifestFromWidePart(const IMergeTreeDataPart & part, const NameAndTypePair & column);

/// Sorted union of source manifests. Duplicate keys keep the first entry's kinds.
MapKeyManifest unionMapKeyManifests(const std::vector<MapKeyManifest> & manifests);

/// Stamp a frozen key set onto a `ColumnMap` so `collectManifestFromColumn` does not
/// rescan the first written block.
void stampMapKeyUnion(ColumnMap & column, const MapKeyManifest & manifest);

/// Remap one row of part-order presence (`source_size` UInt8s) into union order.
void remapPresenceRow(
    const UInt8 * source,
    size_t source_size,
    const std::vector<ssize_t> & union_to_source,
    PaddedPODArray<UInt8> & dest);

std::vector<ssize_t> buildPresenceRemap(const MapKeyManifest & source, const MapKeyManifest & union_manifest);

String mapKeySubcolumnName(const String & map_column, const String & key_stream_name);
String mapKeysPresenceSubcolumnName(const String & map_column);

}
