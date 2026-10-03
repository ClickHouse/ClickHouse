#include <Storages/MergeTree/MapWithKeyColumnsMerge.h>

#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/assert_cast.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <map>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
}

bool partUsesMapWithKeyColumns(const IMergeTreeDataPart & part, const String & column_name)
{
    auto serialization = part.getSerialization(column_name);
    return typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get()) != nullptr;
}

bool mergeOutputUsesMapWithKeyColumns(const MergeTreeSettings & settings, const IDataType & type)
{
    if (!typeid_cast<const DataTypeMap *>(&type))
        return false;
    return settings[MergeTreeSetting::map_serialization_version] == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
}

String getMapKeyColumnsFileName(const String & name_in_storage, const MergeTreeSettings & settings, const IDataPartStorage * storage)
{
    return replaceFileNameToHashIfNeeded(escapeForFileName(name_in_storage) + ".key_columns.txt", settings, storage);
}

std::unique_ptr<WriteBufferFromFileBase> writeMapKeyColumnsFile(
    IDataPartStorage & storage,
    const String & column_name,
    const MergeTreeSettings & storage_settings,
    const DataTypePtr & key_type,
    const MapKeyManifest & manifest,
    const WriteSettings & query_write_settings,
    MergeTreeDataPartChecksums & checksums)
{
    const auto file_name = getMapKeyColumnsFileName(column_name, storage_settings, &storage);
    auto out = storage.writeFile(file_name, 4096, query_write_settings);
    HashingWriteBuffer hashing(*out);
    SerializationMapWithKeyColumns::writeKeyColumnsText(hashing, key_type, manifest);
    hashing.finalize();
    checksums.addFile(file_name, hashing.count(), hashing.getHash());
    out->preFinalize();
    return out;
}

MapKeyManifest readMapKeyManifestFromPart(const IMergeTreeDataPart & part, const NameAndTypePair & column)
{
    if (const auto * manifest = part.tryGetMapKeyColumnsManifest(column.name))
        return *manifest;
    return {};
}

MapKeyManifest unionMapKeyManifests(const std::vector<MapKeyManifest> & manifests)
{
    std::map<Field, MapKeyManifestEntry> unique;
    for (const auto & manifest : manifests)
    {
        for (const auto & entry : manifest.keys)
        {
            if (!unique.contains(entry.key))
                unique.emplace(entry.key, entry);
        }
    }

    MapKeyManifest result;
    result.keys.reserve(unique.size());
    for (auto & [_, entry] : unique)
        result.keys.push_back(std::move(entry));
    return result;
}

void stampMapKeyUnion(ColumnMap & column, const MapKeyManifest & manifest)
{
    auto stats = column.getStatistics()
        ? std::make_shared<ColumnMap::Statistics>(*column.getStatistics())
        : std::make_shared<ColumnMap::Statistics>();
    stats->collect_keys = true;
    stats->keys.clear();
    stats->keys.reserve(manifest.keys.size());
    for (const auto & entry : manifest.keys)
        stats->keys.push_back(entry.key);
    column.setStatistics(stats);
}

String mapKeySubcolumnName(const String & map_column, const String & key_stream_name)
{
    return map_column + "." + String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + key_stream_name;
}

String mapKeyExistsSubcolumnName(const String & map_column, const String & key_stream_name)
{
    return map_column + "." + String(DataTypeMap::EXISTS_SUBCOLUMN_PREFIX) + key_stream_name;
}

}
