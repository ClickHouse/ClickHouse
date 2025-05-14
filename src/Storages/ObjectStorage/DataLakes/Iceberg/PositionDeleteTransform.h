#pragma once

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>

namespace DB
{

class IcebergPositionDeleteTransform : public ISimpleTransform
{
public:
    static constexpr const char * positions_column_name = "pos";
    static constexpr const char * filename_column_name = "file_path";

    IcebergPositionDeleteTransform(
        const Block & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        const ObjectStoragePtr object_storage_,
        const IcebergMetadata::ConfigurationPtr configuration_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_)
        : ISimpleTransform(header_, header_, false)
        , header(header_)
        , iceberg_object_info(iceberg_object_info_)
        , object_storage(object_storage_)
        , configuration(configuration_)
        , format_settings(format_settings_)
        , context(context_)
    {
        initializeDeleteSources();
    }

    virtual String getName() const override { return "IcebergPositionDeleteTransform"; }

    static size_t getDeleteFilenameColumnIndex(const std::shared_ptr<IInputFormat> & delete_source);
    static size_t getDeletePositionColumnIndex(const std::shared_ptr<IInputFormat> & delete_source);

private:
    void initializeDeleteSources();

protected:
    LoggerPtr log = getLogger("IcebergPositionDeleteTransform");

    Block header;
    IcebergDataObjectInfoPtr iceberg_object_info;
    const ObjectStoragePtr object_storage;
    const IcebergMetadata::ConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    ContextPtr context;

    /// We need to keep the read buffers alive since the delete_sources depends on them.
    std::vector<std::unique_ptr<ReadBuffer>> delete_read_buffers;
    std::vector<std::shared_ptr<IInputFormat>> delete_sources;
};

class IcebergBitmapPositionDeleteTransform : public IcebergPositionDeleteTransform
{
public:
    IcebergBitmapPositionDeleteTransform(
        const Block & header_,
        IcebergDataObjectInfoPtr iceberg_object_info_,
        const ObjectStoragePtr object_storage_,
        const IcebergMetadata::ConfigurationPtr configuration_,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_)
        : IcebergPositionDeleteTransform(header_, iceberg_object_info_, object_storage_, configuration_, format_settings_, context_)
    {
        initialize();
    }

    String getName() const override { return "IcebergBitmapPositionDeleteTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    void initialize();
    RoaringBitmapWithSmallSet<size_t, 32> bitmap;
};

}
