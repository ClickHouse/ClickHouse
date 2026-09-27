#pragma once

#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

#include <memory>
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

/// `<column>.key_columns.txt`, using the same escaping and long-name hashing as column data files.
String getMapKeyColumnsFileName(const String & name_in_storage, const MergeTreeSettings & settings, const IDataPartStorage * storage);

/// Write one plain-text key list and record its uncompressed checksum. The returned buffer is
/// pre-finalized; the caller syncs and finalizes it with the rest of the part.
std::unique_ptr<WriteBufferFromFileBase> writeMapKeyColumnsFile(
    IDataPartStorage & storage,
    const String & column_name,
    const MergeTreeSettings & storage_settings,
    const DataTypePtr & key_type,
    const MapKeyManifest & manifest,
    const WriteSettings & query_write_settings,
    MergeTreeDataPartChecksums & checksums);

/// Key list loaded when the part was opened, from `<column>.key_columns.txt`.
MapKeyManifest readMapKeyManifestFromPart(const IMergeTreeDataPart & part, const NameAndTypePair & column);

/// Sorted union of source manifests. Duplicate keys keep the first entry's kinds.
MapKeyManifest unionMapKeyManifests(const std::vector<MapKeyManifest> & manifests);

/// Stamp a frozen key set onto a `ColumnMap` so `collectManifestFromColumn` does not
/// rescan the first written block.
void stampMapKeyUnion(ColumnMap & column, const MapKeyManifest & manifest);

String mapKeySubcolumnName(const String & map_column, const String & key_stream_name);
String mapKeyExistsSubcolumnName(const String & map_column, const String & key_stream_name);

}
