#include <Backups/BackupEntryFromMemory.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

BackupEntryFromMemory::BackupEntryFromMemory(const void * data_, size_t size_)
    : BackupEntryFromMemory(String{reinterpret_cast<const char *>(data_), size_})
{
}

BackupEntryFromMemory::BackupEntryFromMemory(String data_) : data(std::move(data_))
{
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromMemory::getReadBuffer(const ReadSettings &) const
{
    return std::make_unique<ReadBufferFromString>(data);
}

}
