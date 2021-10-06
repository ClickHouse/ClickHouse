#include <Disks/BlobStorage/DiskBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE

#include <iostream>
#include <random>
#include <common/logger_useful.h>

namespace DB
{


// // TODO: abstract this function from DiskS3.cpp, from where it was copy-pasted
// String getRandomName()
// {
//     std::uniform_int_distribution<int> distribution('a', 'z');
//     String res(32, ' '); /// The number of bits of entropy should be not less than 128.
//     for (auto & c : res)
//         c = distribution(thread_local_rng);
//     return res;
// }


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
    ReadIndirectBufferFromBlobStorage(
        Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
        IDiskRemote::Metadata metadata_,
        size_t buf_size_) :
        ReadIndirectBufferFromRemoteFS<ReadBufferFromBlobStorage>(metadata_),
        blob_container_client(blob_container_client_),
        buf_size(buf_size_)
    {}

    std::unique_ptr<ReadBufferFromBlobStorage> createReadBuffer(const String & path) override
    {
        return std::make_unique<ReadBufferFromBlobStorage>(blob_container_client, metadata.remote_fs_root_path + path, buf_size);
    }

private:
    Azure::Storage::Blobs::BlobContainerClient blob_container_client;
    size_t buf_size;
};


DiskBlobStorage::DiskBlobStorage(
    const String & name_,
    const String & metadata_path_,
    Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
    size_t thread_pool_size_) :
    IDiskRemote(name_, "" /* TODO: shall we provide a config for this path? */, metadata_path_, "DiskBlobStorage", thread_pool_size_),
    blob_container_client(blob_container_client_)
{

}


std::unique_ptr<ReadBufferFromFileBase> DiskBlobStorage::readFile(
    const String & path,
    size_t buf_size,
    size_t,
    size_t,
    size_t,
    MMappedFileCache *) const
{
    auto metadata = readMeta(path);

    LOG_DEBUG(log, "Read from file by path: {}. Existing Blob Storage objects: {}",
        backQuote(metadata_path + path), metadata.remote_fs_objects.size());

    auto reader = std::make_unique<ReadIndirectBufferFromBlobStorage>(blob_container_client, metadata, buf_size);

    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), buf_size); // TODO: last one is the min bytes read, to change
}


std::unique_ptr<WriteBufferFromFileBase> DiskBlobStorage::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode)
{
    auto metadata = readOrCreateMetaForWriting(path, mode);
    auto blob_path = path; // TODO: maybe use getRandomName() or modify the path (now it contains the tmp_* directory part)

    LOG_DEBUG(log, "{} to file by path: {}. Blob Storage path: {}",
        mode == WriteMode::Rewrite ? "Write" : "Append", backQuote(metadata_path + path), remote_fs_root_path + blob_path);

    auto buffer = std::make_unique<WriteBufferFromBlobStorage>(blob_container_client, metadata.remote_fs_root_path + blob_path, buf_size);

    return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromBlobStorage>>(std::move(buffer), std::move(metadata), blob_path);
}


DiskType::Type DiskBlobStorage::getType() const
{
    return DiskType::Type::BlobStorage;
}


bool DiskBlobStorage::supportZeroCopyReplication() const
{
    return false;
}


bool DiskBlobStorage::checkUniqueId(const String &) const
{
    return false;
}


void DiskBlobStorage::removeFromRemoteFS(RemoteFSPathKeeperPtr)
{

}


RemoteFSPathKeeperPtr DiskBlobStorage::createFSPathKeeper() const
{
    return std::make_shared<BlobStoragePathKeeper>(1024);
}


void blob_storage_demo()
{
    // to not repeat it for every storage function
    using namespace Azure::Storage::Blobs;

    // obtain the managed identity, it should be available in the VM
    // quote from sdk/identity/azure-identity/samples/managed_identity_credential.cpp:
    // "Managed Identity Credential would be available in some environments such as on Azure VMs."
    auto managedIdentityCredential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();

    // url follows the format "http://*mystorageaccount*.blob.core.windows.net/*mycontainer*/*myblob*",
    // with blob name and container name being optional
    // here we only specify the account "stor_acc" and container "cont"
    auto url = "https://sadttmpstgeus.blob.core.windows.net/data";

    // create a client accessing the container "cont"
    auto blobContainerClient = BlobContainerClient(url, managedIdentityCredential);

    // list blobs in the container
    auto listBlobs = blobContainerClient.ListBlobs();

    // print information about the container
    std::cout << "Storage account: " << listBlobs.ServiceEndpoint
        << ", container: " << listBlobs.BlobContainerName << "\n";

    // print information about the blobs in the container
    std::cout << "Blobs (max 20):\n";
    for (size_t i = 0; i < 20 && i < listBlobs.Blobs.size(); i++)
    {
        auto & x = listBlobs.Blobs[i];
        std::cout << x.Name << " " << x.BlobSize << " " << x.BlobType.ToString() << "\n";
    }
    std::cout << "\n";

    // change url to point to a blob
    url = "https://sadttmpstgeus.blob.core.windows.net/data/hello";

    // create a client accessing the blob
    auto blobClient = BlobClient(url, managedIdentityCredential);

    // obtain properties of the blob
    auto blobProperties = blobClient.GetProperties();

    // print the creation date for the blob
    std::cout << blobProperties.Value.CreatedOn.ToString() << "\n";

    // create a client to manipulate the blob
    auto blockBlobClient = BlockBlobClient(url, managedIdentityCredential);

    // data to be put in the blob
    const uint8_t dataSample[] = {1, 2, 3};

    // overwrite "file.txt" blob with the data above
    blockBlobClient.UploadFrom(dataSample, 3);

    // get list of blocks within the block
    auto blobList = blockBlobClient.GetBlockList();

    // should print a recent time and size 3
    std::cout << "Last modified date of uploaded blob: " << blobList.Value.LastModified.ToString()
        << ", size: " << blobList.Value.BlobSize << "\n";

    // TODO: make sure "file.txt" exists or create it

    // // overwrite "file.txt" blob with the data from the file "file.txt" in the same directory
    // blockBlobClient.UploadFrom("file.txt");

    // // once again, get list of blocks within the block
    // blobList = blockBlobClient.GetBlockList();

    // // should print a recent time and the size of file.txt
    // std::cout << "Last modified date of uploaded blob: " << blobList.Value.LastModified.ToString()
    //     << ", size: " << blobList.Value.BlobSize << "\n";
}

}

#endif
