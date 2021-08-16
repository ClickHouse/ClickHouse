#include "WriteIndirectBufferFromRemoteFS.h"

#if USE_AWS_S3 || USE_HDFS
#include <IO/WriteBufferFromS3.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>


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
        WriteIndirectBufferFromRemoteFS::finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


template <typename T>
void WriteIndirectBufferFromRemoteFS<T>::finalize()
{
    if (finalized)
        return;

    WriteBufferFromFileDecorator::finalize();

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

#if USE_HDFS
template
class WriteIndirectBufferFromRemoteFS<WriteBufferFromHDFS>;
#endif

}

#endif
