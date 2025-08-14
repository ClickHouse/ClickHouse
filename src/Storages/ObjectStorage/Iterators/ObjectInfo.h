
#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

namespace DB {

struct ObjectInfo
{
    virtual std::string getFileName() const = 0;
    virtual std::string getPath() const = 0;
    virtual bool isArchive() const = 0;
    virtual std::string getPathToArchive() const = 0;
    virtual size_t fileSizeInArchive() const = 0;
    virtual std::string getPathOrPathToArchiveIfArchive() const = 0;
    virtual bool hasPositionDeleteTransformer() const = 0;
    virtual bool suitableForNumsRowCache() const = 0;
    virtual ~ObjectInfo() = default;
    virtual RelativePathWithMetadata& getUnderlyingObject() = 0;
    virtual const RelativePathWithMetadata& getUnderlyingObject() const = 0;
    virtual std::optional<DataLakeObjectMetadata>& getDataLakeMetadata() = 0;
    virtual void setDataLakeMetadata(std::optional<DataLakeObjectMetadata> metadata) = 0;


};


using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
using ObjectInfos = std::vector<ObjectInfoPtr>;

struct ObjectInfoOneFile : public ObjectInfo {

    ObjectInfoOneFile(String relative_path_, std::optional<ObjectMetadata> metadata_)
        : relative_path_with_metadata(std::move(relative_path_), std::move(metadata_)) {}

    RelativePathWithMetadata& getUnderlyingObject() override {
        return relative_path_with_metadata;
    }
    const RelativePathWithMetadata& getUnderlyingObject() const override {
        return relative_path_with_metadata;
    }
    std::string getPath() const override  { return relative_path_with_metadata.getPath(); }
    std::string getFileName() const override { return std::filesystem::path(getPath()).filename(); }
    bool isArchive() const override { return false; }
    std::string getPathToArchive() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    size_t fileSizeInArchive() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not an archive"); }
    std::string getPathOrPathToArchiveIfArchive() const override
    {
        return getPath();
    }
private:
    RelativePathWithMetadata relative_path_with_metadata;
};

struct ObjectInfoPlain : public ObjectInfoOneFile
{
    ObjectInfoPlain(String relative_path_, std::optional<ObjectMetadata> metadata_)
        : ObjectInfoOneFile(std::move(relative_path_), std::move(metadata_)) {}

    bool hasPositionDeleteTransformer() const override { return false; }
    bool suitableForNumsRowCache() const override { return true; }
    std::optional<DataLakeObjectMetadata>& getDataLakeMetadata() override  {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }
    void setDataLakeMetadata(std::optional<DataLakeObjectMetadata>) override {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }
};

struct ObjectInfoInArchive : public ObjectInfo
{
    ObjectInfoInArchive(
        RelativePathWithMetadata archive_object_,
        const std::string & path_in_archive_,
        std::shared_ptr<IArchiveReader> archive_reader_,
        IArchiveReader::FileInfo && file_info_);

    RelativePathWithMetadata& getUnderlyingObject() override {
        return underlying_blob_object;
    }
    const RelativePathWithMetadata& getUnderlyingObject() const override {
        return underlying_blob_object;
    }

    bool isArchive() const override { return true; }
    std::string getFileName() const override
    {
        return path_in_archive;
    }

    std::string getPath() const override
    {
        return underlying_blob_object.getPath() + "::" + path_in_archive;
    }

    std::string getPathToArchive() const override
    {
        return underlying_blob_object.getPath();
    }

    std::string getPathOrPathToArchiveIfArchive() const override
    {
        return getPathToArchive();
    }

    size_t fileSizeInArchive() const override { return file_info.uncompressed_size; }

    bool suitableForNumsRowCache() const override { return false; }

    bool hasPositionDeleteTransformer() const override { return false; }

    std::optional<DataLakeObjectMetadata>& getDataLakeMetadata() override  {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }
    void setDataLakeMetadata(std::optional<DataLakeObjectMetadata>) override {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not supported for plain object info");
    }

    std::unique_ptr<ReadBufferFromFileBase> getReadBuf() const {
        return archive_reader->readFile(path_in_archive, /*throw_on_not_found=*/true);
    }

private:
    RelativePathWithMetadata underlying_blob_object;
    const std::string path_in_archive;
    const std::shared_ptr<IArchiveReader> archive_reader;
    const IArchiveReader::FileInfo file_info;
};

}
