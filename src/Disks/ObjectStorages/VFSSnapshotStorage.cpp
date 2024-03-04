#include "VFSSnapshotStorage.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

VFSSnapshotEntry VFSSnapshotEntryStringSerializer::deserialize(ReadBuffer & read_buffer)
{
    VFSSnapshotEntry entry;

    readStringUntilWhitespace(entry.remote_path, read_buffer);
    checkChar(' ', read_buffer);
    readIntTextUnsafe(entry.link_count, read_buffer);
    checkChar('\n', read_buffer);

    return entry;
}

void VFSSnapshotEntryStringSerializer::serialize(const VFSSnapshotEntry & entry, WriteBuffer & write_buffer)
{
    const String str = fmt::format("{} {}\n", entry.remote_path, entry.link_count);
    writeString(str, write_buffer);
}


VFSSnapshotSortingWriteStream::VFSSnapshotSortingWriteStream(VFSSnapshotWriteStreamPtr write_stream_) : write_stream(write_stream_)
{
}

void VFSSnapshotSortingWriteStream::writeImpl(VFSSnapshotEntry && entry)
{
    // TODO (alexfvk): external sorting/merging with fixed memory usage
    // NOTE: In case of external sorting handle duplicates while merging sorted chunks
    if (auto it = sorted_entries.find(entry); it == sorted_entries.end())
        sorted_entries.emplace(std::move(entry));
    else
        it->link_count += entry.link_count;
}

void VFSSnapshotSortingWriteStream::finalizeImpl()
{
    for (auto && entry : sorted_entries.extract_sequence())
        write_stream->write(std::move(entry));
    write_stream->finalize();
}

};
