
#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB {

// Represents an object from which we can create an InputFormat.
// Can denote an object in blob storage, part of an archive which is stored in blob storage.
// Can contain information about transforms which are required to be applied to a format.
struct ObjectInfoBase
{
    RelativePathWithMetadata base_object_info;

    explicit ObjectInfoBase(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : base_object_info{std::move(relative_path_), std::move(metadata_)}
    {
    }

    ObjectInfoBase(const ObjectInfoBase & other) = default;

    bool hasBaseBlobMetadata() const { return base_object_info.metadata.has_value(); }
    const std::optional<ObjectMetadata> & tryGetBaseBlobMetadata() const { return base_object_info.metadata; }
    const ObjectMetadata & getBaseBlobMetadata() const {return *base_object_info.metadata; }
    void setBaseBlobMetadata(ObjectMetadata metadata) { base_object_info.metadata = std::move(metadata); }
    void downloadBaseBlobMetadataIfNotSet(DB::ObjectStoragePtr object_storage)
    {
        if (!base_object_info.metadata.has_value())
        {
            base_object_info.metadata = object_storage->getObjectMetadata(base_object_info.relative_path);
        }
    }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath() const {
        return nullptr;
    }

    virtual ~ObjectInfoBase() = default;
    virtual std::string getPath() const;
    virtual std::string getFileName() const;

    virtual bool isArchive() const { return false; }
    virtual std::string getPathToArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual size_t fileSizeInArchive() const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    virtual std::string getPathOrPathToArchiveIfArchive() const;

    virtual bool suitableForNumsRowCache() const;

    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer() const { return {}; }
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchema() const { return {}; }

    virtual std::optional<DataLakeObjectMetadata> getDataLakeMetadata() const { return std::nullopt; }

    virtual bool hasPositionDeleteTransformer() const { return false; }
    virtual std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const SharedHeader & /*header*/, const std::optional<FormatSettings> & /*format_settings*/, ContextPtr /*context_*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'getPositionDeleteTransformer' is not implemented for object info type");
    }
};
using ObjectInfoPtr = std::shared_ptr<ObjectInfoBase>;
using ObjectInfos = std::vector<ObjectInfoPtr>;

struct ObjectInfoPlain : public ObjectInfoBase {
    explicit ObjectInfoPlain(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : ObjectInfoBase(std::move(relative_path_), std::move(metadata_))
    {
    }

    explicit ObjectInfoPlain(const RelativePathWithMetadata & object_info)
        : ObjectInfoBase(object_info.relative_path, object_info.metadata)
    {
    }

    std::string getPath() const override  { return base_object_info.relative_path; }
    std::string getFileName() const override { return std::filesystem::path(getPath()).filename(); }
    bool suitableForNumsRowCache() const override { return true; }
};


}
