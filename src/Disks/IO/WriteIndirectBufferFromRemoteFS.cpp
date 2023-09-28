#include "WriteIndirectBufferFromRemoteFS.h"

namespace DB
{

WriteIndirectBufferFromRemoteFS::WriteIndirectBufferFromRemoteFS(
    std::unique_ptr<WriteBuffer> impl_,
    CreateMetadataCallback && create_callback_,
    const String & remote_path_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , create_metadata_callback(std::move(create_callback_))
    , remote_path(remote_path_)
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
    if (create_metadata_callback)
        create_metadata_callback(count());
}


}
