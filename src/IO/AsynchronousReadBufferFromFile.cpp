#include <IO/AsynchronousReadBufferFromFile.h>

namespace DB
{

AsynchronousReadBufferFromFileWithDescriptorsCache::~AsynchronousReadBufferFromFileWithDescriptorsCache()
{
    /// Must wait for events in flight before potentially closing the file by destroying OpenedFilePtr.
    finalize();
}

}
