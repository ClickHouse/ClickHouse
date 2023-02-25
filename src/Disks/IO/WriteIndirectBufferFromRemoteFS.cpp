#include "WriteIndirectBufferFromRemoteFS.h"

#include <IO/WriteBufferFromS3.h>
#include <IO/WriteBufferFromAzureBlobStorage.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <IO/WriteBufferFromHTTP.h>


namespace DB
{

WriteIndirectBufferFromRemoteFS::WriteIndirectBufferFromRemoteFS(
    std::unique_ptr<WriteBuffer> impl_,
    CreateMetadataCallback && create_callback_,
    const String & metadata_file_path_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , create_metadata_callback(std::move(create_callback_))
    , metadata_file_path(metadata_file_path_)
{
}


WriteIndirectBufferFromRemoteFS::~WriteIndirectBufferFromRemoteFS()
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


void WriteIndirectBufferFromRemoteFS::finalizeImpl()
{
    WriteBufferFromFileDecorator::finalizeImpl();
    create_metadata_callback(count());
}


}
