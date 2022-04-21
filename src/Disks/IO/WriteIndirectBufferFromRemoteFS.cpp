#include "WriteIndirectBufferFromRemoteFS.h"

#include <IO/WriteBufferFromS3.h>
#include <IO/WriteBufferFromAzureBlobStorage.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <IO/WriteBufferFromHTTP.h>


namespace DB
{

template <typename T>
WriteIndirectBufferFromRemoteFS<T>::WriteIndirectBufferFromRemoteFS(
    std::unique_ptr<T> impl_,
    CreateMetadataCallback && create_callback_,
    const String & metadata_file_path_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , create_metadata_callback(std::move(create_callback_))
    , metadata_file_path(metadata_file_path_)
{
}

template <typename T>
WriteIndirectBufferFromRemoteFS<T>::~WriteIndirectBufferFromRemoteFS()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

template <typename T>
void WriteIndirectBufferFromRemoteFS<T>::finalizeImpl()
{
    WriteBufferFromFileDecorator::finalizeImpl();
    create_metadata_callback(count());
}

#if USE_AWS_S3
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromS3>;
#endif

#if USE_AZURE_BLOB_STORAGE
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromAzureBlobStorage>;
#endif

#if USE_HDFS
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromHDFS>;
#endif

template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromHTTP>;

}
