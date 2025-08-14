
#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB {

// Represents an object from which we can create an InputFormat.
// Can denote an object in blob storage, part of an archive which is stored in blob storage.
// Can contain information about transforms which are required to be applied to a format.
struct ReadableObject
{
    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath() const {
        return nullptr;
    }

    virtual std::string getPath() const = 0;
    virtual std::string getFileName() const = 0;
    virtual bool isArchive() const = 0;
    virtual std::string getPathToArchive() const = 0;
    virtual size_t fileSizeInArchive() const = 0;
    virtual std::string getPathOrPathToArchiveIfArchive() const = 0;
    virtual bool suitableForNumsRowCache() const = 0;
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer() const = 0;
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchema() const = 0;
    virtual std::optional<DataLakeObjectMetadata> getDataLakeMetadata() const = 0;
    virtual bool hasPositionDeleteTransformer() const = 0;
    virtual std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        const SharedHeader & /*header*/, const std::optional<FormatSettings> & /*format_settings*/, ContextPtr /*context_*/) const
        = 0;
    virtual ~ReadableObject() = default;
};
using ObjectInfoPtr = std::shared_ptr<ReadableObject>;
using ObjectInfos = std::vector<ObjectInfoPtr>;

struct OneFileReadableObject : public ReadableObject
{
    std::string getPath() override const = 0;
    virtual std::string getFileName() const = 0;
    virtual bool isArchive() const = 0;
    virtual std::string getPathToArchive() const = 0;
    virtual size_t fileSizeInArchive() const = 0;
    virtual std::string getPathOrPathToArchiveIfArchive() const = 0;
    virtual bool suitableForNumsRowCache() const = 0;
    virtual std::shared_ptr<const ActionsDAG> getSchemaTransformer() const = 0;
    virtual std::shared_ptr<NamesAndTypesList> getInitialSchema() const = 0;
    virtual std::optional<DataLakeObjectMetadata> getDataLakeMetadata() const = 0;
    virtual bool hasPositionDeleteTransformer() const = 0;
    explicit OneFileReadableObject(String relative_path_, std::optional<ObjectMetadata> metadata_ = std::nullopt)
        : ReadableObject(std::move(relative_path_), std::move(metadata_))
    {
    }

    explicit ObjectInfoPlain(const RelativePathWithMetadata & object_info)
        : ReadableObject(object_info.relative_path, object_info.metadata)
    {
    }

    std::string getPath() const override  { return base_object_info.relative_path; }
    std::string getFileName() const override { return std::filesystem::path(getPath()).filename(); }
    bool suitableForNumsRowCache() const override { return true; }
};
}
