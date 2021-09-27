#include "Blob.h"

#if USE_AZURE_BLOB_STORAGE

#include <iostream>
#include <azure/storage/blobs.hpp>
#include <azure/identity/managed_identity_credential.hpp>


namespace DB
{

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
