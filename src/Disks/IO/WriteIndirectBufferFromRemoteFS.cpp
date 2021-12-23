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
    IDiskRemote::Metadata metadata_,
    const String & remote_fs_path_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , metadata(std::move(metadata_))
    , remote_fs_path(remote_fs_path_)
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

    metadata.addObject(remote_fs_path, count());
    metadata.save();
}


template <typename T>
void WriteIndirectBufferFromRemoteFS<T>::sync()
{
    if (finalized)
        metadata.save(true);
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
