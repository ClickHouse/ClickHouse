#include <Disks/IO/WriteBufferWithFinalizeCallback.h>

namespace DB
{

WriteBufferWithFinalizeCallback::WriteBufferWithFinalizeCallback(
    std::unique_ptr<WriteBuffer> impl_,
    FinalizeCallback && create_callback_,
    const String & remote_path_,
    bool do_not_write_empty_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , create_metadata_callback(std::move(create_callback_))
    , remote_path(remote_path_)
    , do_not_write_empty(do_not_write_empty_)
{
}

void WriteBufferWithFinalizeCallback::preFinalize()
{
    /// Underlying buffer can initiate write to the storage in its preFinalize.
    /// So we skip this call if the buffer is empty and do_not_write_empty optimization is enabled.
    const auto bytes_written = count();
    if (do_not_write_empty && bytes_written == 0)
        return;

    WriteBufferFromFileDecorator::preFinalize();
}

void WriteBufferWithFinalizeCallback::finalizeImpl()
{
    const auto bytes_written = count();
    if (do_not_write_empty && bytes_written == 0)
        WriteBufferFromFileDecorator::cancel();
    else
        WriteBufferFromFileDecorator::finalizeImpl();

    if (create_metadata_callback)
        create_metadata_callback(bytes_written);
}


}
