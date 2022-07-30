#include <Backups/BackupEntryFromMemory.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

BackupEntryFromMemory::BackupEntryFromMemory(const void * data_, size_t size_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(String{reinterpret_cast<const char *>(data_), size_}, checksum_)
{
}

BackupEntryFromMemory::BackupEntryFromMemory(String data_, const std::optional<UInt128> & checksum_)
    : data(std::move(data_)), checksum(checksum_)
{
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromMemory::getReadBuffer() const
{
    return std::make_unique<ReadBufferFromString>(data);
}

}
