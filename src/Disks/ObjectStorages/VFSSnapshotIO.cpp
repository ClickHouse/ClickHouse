#include "VFSSnapshotIO.h"
#include "IO/ReadBufferFromFileBase.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"
#include "IObjectStorage.h"
#include "StoredObject.h"

namespace ProfileEvents
{
extern const Event VFSGcCumulativeSnapshotBytesRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

// weak ordering is used for sorting/finding entries via flat_set
bool VFSSnapshotEntry::operator<(const VFSSnapshotEntry & entry) const
{
    return remote_path < entry.remote_path;
}

bool VFSSnapshotEntry::operator==(const VFSSnapshotEntry & entry) const
{
    return remote_path == entry.remote_path;
}

VFSSnapshotEntry VFSSnapshotEntry::deserialize(ReadBuffer & buf)
{
    VFSSnapshotEntry entry;

    readStringUntilWhitespace(entry.remote_path, buf);
    checkChar(' ', buf);
    readIntTextUnsafe(entry.link_count, buf);
    checkChar('\n', buf);

    return entry;
}

void VFSSnapshotEntry::serialize(WriteBuffer & buf)
{
    writeString(fmt::format("{} {}\n", remote_path, link_count), buf);
}

IVFSSnapshotReadStream::Entry IVFSSnapshotReadStream::next()
{
    if (finished)
        return {};
    if (Entry res = impl())
        return res;
    finished = true;
    return {};
}

VFSSnapshotReadStream::VFSSnapshotReadStream(IObjectStorage & storage, const StoredObject & obj) : buf(storage.readObject(obj))
{
}

VFSSnapshotReadStream::Entry VFSSnapshotReadStream::impl()
{
    if (buf.eof())
    {
        ProfileEvents::increment(ProfileEvents::VFSGcCumulativeSnapshotBytesRead, buf.count());
        return {};
    }
    return VFSSnapshotEntry::deserialize(buf);
}

VFSSnapshotReadStreamFromString::VFSSnapshotReadStreamFromString(std::string_view data) : buf(data)
{
}

VFSSnapshotReadStreamFromString::Entry VFSSnapshotReadStreamFromString::impl()
{
    if (buf.eof())
        return {};
    return VFSSnapshotEntry::deserialize(buf);
}

void IVFSSnapshotWriteStream::write(VFSSnapshotEntry && entry)
{
    if (finished)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot write stream already finished");
    impl(std::move(entry));
}

// TODO myrrc research zstd dictionary builder or zstd for compression
VFSSnapshotWriteStream::VFSSnapshotWriteStream(IObjectStorage & storage, const StoredObject & obj, int level)
    : buf(storage.writeObject(obj, WriteMode::Rewrite), level)
{
}

void VFSSnapshotWriteStream::impl(VFSSnapshotEntry && entry)
{
    entry.serialize(buf);
}

void VFSSnapshotWriteStream::finalizeImpl()
{
    buf.finalize();
}

void IVFSSnapshotWriteStream::finalize()
{
    finalizeImpl();
    finished = true;
}

VFSSnapshotSortingWriteStream::VFSSnapshotSortingWriteStream(IVFSSnapshotWriteStream & stream_) : stream(stream_)
{
}

void VFSSnapshotSortingWriteStream::impl(VFSSnapshotEntry && entry)
{
    // TODO (alexfvk): external sorting/merging with fixed memory usage
    // NOTE: In case of external sorting handle duplicates while merging sorted chunks
    if (auto it = entries.find(entry); it == entries.end())
        entries.emplace(std::move(entry));
    else
        it->link_count += entry.link_count;
}

void VFSSnapshotSortingWriteStream::finalizeImpl()
{
    for (auto && entry : entries.extract_sequence())
        stream.write(std::move(entry));
    stream.finalize();
}
}
