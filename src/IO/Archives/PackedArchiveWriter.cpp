#include <IO/Archives/PackedArchiveWriter.h>

#include <IO/PackedFilesWriter.h>
#include <IO/WriteBuffer.h>
#include <IO/copyData.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

PackedArchiveWriter::PackedArchiveWriter(std::unique_ptr<WriteBuffer> out_, VectorWithMemoryTracking<Member> members_, UInt8 version_)
    : out(std::move(out_))
    , members(std::move(members_))
{
    Strings names;
    names.reserve(members.size());
    for (const auto & member : members)
        names.push_back(member.name);

    /// Bodies start right after the serialized front index; offsets are derived from the sizes.
    UInt64 offset = PackedFilesWriter::getSerializedIndexSize(names, version_);
    PackedFilesIO::Index index;
    for (const auto & member : members)
    {
        if (!index.emplace(member.name, PackedFilesIO::FileOffset{offset, member.size, 0}).second)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Duplicate member {} in packed archive", member.name);
        offset += member.size;
    }

    PackedFilesWriter::writePackedIndex(*out, index, version_);
}

void PackedArchiveWriter::writeMember(const String & name, ReadBuffer & in)
{
    if (next_member >= members.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "All {} members of the packed archive have already been written", members.size());

    const auto & member = members[next_member];
    if (name != member.name)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Packed archive members must be written in manifest order: expected {}, got {}", member.name, name);

    /// copyData writes exactly member.size bytes (throwing if the source is short), keeping the body
    /// consistent with the offset/size already recorded in the front index.
    copyData(in, *out, member.size);
    ++next_member;
}

void PackedArchiveWriter::finalize()
{
    if (next_member != members.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot finalize packed archive: only {} of {} members written", next_member, members.size());
    out->finalize();
    finalized = true;
}

void PackedArchiveWriter::cancel() noexcept
{
    out->cancel();
}

PackedArchiveWriter::~PackedArchiveWriter()
{
    if (!finalized)
        out->cancel();
}

}
