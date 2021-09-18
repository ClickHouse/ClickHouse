#include <Backups/BackupEntryConcat.h>
#include <IO/ConcatReadBuffer.h>


namespace DB
{
BackupEntryConcat::BackupEntryConcat(
    BackupEntryPtr first_source_,
    BackupEntryPtr second_source_,
    const std::optional<UInt128> & checksum_)
    : first_source(std::move(first_source_))
    , second_source(std::move(second_source_))
    , checksum(checksum_)
{
}

UInt64 BackupEntryConcat::getSize() const
{
    if (!size)
        size = first_source->getSize() + second_source->getSize();
    return *size;
}

std::unique_ptr<ReadBuffer> BackupEntryConcat::getReadBuffer() const
{
    return std::make_unique<ConcatReadBuffer>(*first_source->getReadBuffer(), *second_source->getReadBuffer());
}
}
