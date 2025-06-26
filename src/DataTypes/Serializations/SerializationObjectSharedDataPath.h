#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <DataTypes/Serializations/SerializationObjectSharedData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


class SerializationObjectSharedDataPath final : public SerializationWrapper
{
public:
    SerializationObjectSharedDataPath(const SerializationPtr & nested_, SerializationObjectSharedData::Mode mode_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, size_t bucket);

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

private:
    struct PathInfo
    {
        MarkInCompressedFile data_mark;
        MarkInCompressedFile substreams_mark;
        MarkInCompressedFile substreams_marks_mark;
        std::vector<String> substreams;
        std::unordered_map<std::string_view, MarkInCompressedFile> substream_to_mark;
    };

    struct PathsInfos
    {
        std::unordered_map<String, PathInfo> path_to_info;
    };
    
    using PathsInfosGranules = std::vector<PathsInfos>;

    struct SubstreamsCachePathsInfossElement : public ISubstreamsCacheElement
    {
        SubstreamsCachePathsInfossElement(std::shared_ptr<PathsInfosGranules> paths_infos_granules_) : paths_infos_granules(paths_infos_granules_) {}

        std::shared_ptr<PathsInfosGranules> paths_infos_granules;
    };

    std::shared_ptr<PathsInfosGranules> deserializePathsInfos(
        const SerializationObjectSharedData::StructureGranules & structure_granules,
        DeserializeBinaryBulkSettings & settings,
        SubstreamsCache * cache) const;

    SerializationObjectSharedData::Mode mode;
    SerializationPtr serialization_map;
    String path;
    String path_subcolumn;
    DataTypePtr dynamic_type;
    SerializationPtr dynamic_serialization;
    size_t bucket;
};

}
