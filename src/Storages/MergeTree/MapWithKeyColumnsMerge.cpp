#include <Storages/MergeTree/MapWithKeyColumnsMerge.h>

#include <Columns/ColumnMap.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/DataTypeMap.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/assert_cast.h>
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

MapKeyManifest readMapKeyManifestFromWidePart(const IMergeTreeDataPart & part, const NameAndTypePair & column)
{
    auto serialization = part.getSerialization(column.name);
    const auto * with_key_columns = typeid_cast<const SerializationMapWithKeyColumns *>(serialization.get());
    if (!with_key_columns)
        return {};

    ISerialization::SubstreamPath path;
    path.emplace_back(ISerialization::Substream::MapKeysInfo);

    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        return {};

    const String file_name = *stream_name + ".bin";
    const size_t file_size = part.getFileSizeOrZero(file_name);
    if (file_size == 0)
        return {};

    auto file = part.getDataPartStorage().readFile(file_name, {}, file_size);
    CompressedReadBufferFromFile compressed(std::move(file));
    return SerializationMapWithKeyColumns::readManifest(compressed, with_key_columns->getKeySerialization());
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

void remapPresenceRow(
    const UInt8 * source,
    size_t source_size,
    const std::vector<ssize_t> & union_to_source,
    PaddedPODArray<UInt8> & dest)
{
    dest.reserve(dest.size() + union_to_source.size());
    for (ssize_t source_index : union_to_source)
    {
        if (source_index < 0 || static_cast<size_t>(source_index) >= source_size)
            dest.push_back(UInt8(0));
        else
            dest.push_back(source[source_index]);
    }
}

std::vector<ssize_t> buildPresenceRemap(const MapKeyManifest & source, const MapKeyManifest & union_manifest)
{
    std::map<Field, size_t> source_index;
    for (size_t i = 0; i < source.keys.size(); ++i)
        source_index.emplace(source.keys[i].key, i);

    std::vector<ssize_t> remap;
    remap.reserve(union_manifest.keys.size());
    for (const auto & entry : union_manifest.keys)
    {
        auto it = source_index.find(entry.key);
        remap.push_back(it == source_index.end() ? static_cast<ssize_t>(-1) : static_cast<ssize_t>(it->second));
    }
    return remap;
}

String mapKeySubcolumnName(const String & map_column, const String & key_stream_name)
{
    return map_column + "." + String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + key_stream_name;
}

String mapKeysPresenceSubcolumnName(const String & map_column)
{
    return map_column + "." + String(DataTypeMap::KEYS_PRESENCE_SUBCOLUMN);
}

}
