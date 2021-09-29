#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/WriteBufferFromBlobStorage.h>

namespace DB
{

void WriteBufferFromBlobStorage::nextImpl() {
    std::cout << "WriteBufferFromBlobStorage:nextImpl\n\n\n";
}

}

#endif
