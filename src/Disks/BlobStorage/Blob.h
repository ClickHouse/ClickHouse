#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromBlobStorage.h>
#include <IO/WriteBufferFromBlobStorage.h>
#include <Disks/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/WriteIndirectBufferFromRemoteFS.h>
#include <IO/SeekAvoidingReadBuffer.h>


namespace DB
{

void blob_do_sth();

class BlobStoragePathKeeper : public RemoteFSPathKeeper
{
public:
    BlobStoragePathKeeper(size_t chunk_limit_) : RemoteFSPathKeeper(chunk_limit_) {}

    void addPath(const String &) override
    {

    }
};

class ReadIndirectBufferFromBlobStorage final : public ReadIndirectBufferFromRemoteFS<ReadBufferFromBlobStorage>
{
public:
    ReadIndirectBufferFromBlobStorage(IDiskRemote::Metadata metadata_) :
        ReadIndirectBufferFromRemoteFS<ReadBufferFromBlobStorage>(metadata_)
    {}

    std::unique_ptr<ReadBufferFromBlobStorage> createReadBuffer(const String &) override
    {
        return std::make_unique<ReadBufferFromBlobStorage>();
    }
};

class DiskBlobStorage final : public IDiskRemote
{
public:

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String &,
        size_t,
        size_t,
        size_t,
        size_t,
        MMappedFileCache *) const override
    {
        std::string arg1 = "TODO";
        std::string arg2 = "TODO";
        std::string arg3 = "TODO";

        IDiskRemote::Metadata metadata(arg1, arg2, arg3, false);

        auto reader = std::make_unique<ReadIndirectBufferFromBlobStorage>(metadata);
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), 8);
    }

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String &,
        size_t,
        WriteMode) override
    {
        auto buffer = std::make_unique<WriteBufferFromBlobStorage>();
        std::string path = "TODO";

        std::string arg1 = "TODO";
        std::string arg2 = "TODO";
        std::string arg3 = "TODO";

        IDiskRemote::Metadata metadata(arg1, arg2, arg3, false);

        return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromBlobStorage>>(std::move(buffer), std::move(metadata), path);
    }

    DiskType::Type getType() const override
    {
        return DiskType::Type::BlobStorage;
    }

    bool supportZeroCopyReplication() const override
    {
        return false;
    }

    bool checkUniqueId(const String &) const override
    {
        return false;
    }

    void removeFromRemoteFS(RemoteFSPathKeeperPtr) override
    {

    }

    RemoteFSPathKeeperPtr createFSPathKeeper() const override
    {
        return std::make_shared<BlobStoragePathKeeper>(16);
    }

private:

};

}

#endif
