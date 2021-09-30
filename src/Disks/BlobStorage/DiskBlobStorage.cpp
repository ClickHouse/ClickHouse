#include <Disks/BlobStorage/DiskBlobStorage.h>

#if USE_AZURE_BLOB_STORAGE

#include <iostream>


namespace DB
{

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


DiskBlobStorage::DiskBlobStorage(
    const String & name_,
    const String & remote_fs_root_path_,
    const String & metadata_path_,
    const String & log_name_,
    size_t thread_pool_size_) :
    IDiskRemote(name_, remote_fs_root_path_, metadata_path_, log_name_, thread_pool_size_) {}


DiskBlobStorage::DiskBlobStorage() : IDiskRemote("blob_storage", "https://sadttmpstgeus.blob.core.windows.net/data", "/home/jkuklis/blob_storage", "DiskBlobStorage", 1) {}


DiskBlobStorage::DiskBlobStorage(
    const String & name_,
    const String & metadata_path_,
    const String & endpoint_url,
    std::shared_ptr<Azure::Identity::ManagedIdentityCredential>,
    Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
    size_t thread_pool_size_) :
    IDiskRemote(name_, endpoint_url /* or maybe "" ? */, metadata_path_, "DiskBlobStorage", thread_pool_size_)
{
    // list blobs in the container
    auto list_blobs = blob_container_client_.ListBlobs();

    // print information about the container
    std::cout << "Storage account: " << list_blobs.ServiceEndpoint
        << ", container: " << list_blobs.BlobContainerName << "\n\n\n";
}



std::unique_ptr<ReadBufferFromFileBase> DiskBlobStorage::readFile(
    const String &,
    size_t,
    size_t,
    size_t,
    size_t,
    MMappedFileCache *) const
{
    std::string arg1 = "/home/jkuklis/blob_storage/file";
    std::string arg2 = "/home/jkuklis/blob_storage/file";
    std::string arg3 = "/home/jkuklis/blob_storage/file";

    IDiskRemote::Metadata metadata(arg1, arg2, arg3, true);

    auto reader = std::make_unique<ReadIndirectBufferFromBlobStorage>(metadata);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), 32);
}


std::unique_ptr<WriteBufferFromFileBase> DiskBlobStorage::writeFile(
    const String &,
    size_t,
    WriteMode)
{
    auto buffer = std::make_unique<WriteBufferFromBlobStorage>();
    std::string path = "/home/jkuklis/blob_storage/file";

    std::string arg1 = "/home/jkuklis/blob_storage/file";
    std::string arg2 = "/home/jkuklis/blob_storage/file";
    std::string arg3 = "/home/jkuklis/blob_storage/file";

    IDiskRemote::Metadata metadata(arg1, arg2, arg3, true);

    return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromBlobStorage>>(std::move(buffer), std::move(metadata), path);
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


void blob_do_sth()
{
    // not to repeat it for every storage function
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
