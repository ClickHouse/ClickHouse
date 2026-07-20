#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <IO/PackedFilesIO.h>
#include <base/types.h>
#include <memory>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Streaming writer for the PackedFilesIO ".packed" format; output is read back by PackedFilesReader.
/// Unlike PackedFilesWriter (which buffers every member in RAM until finalize), it takes the ordered
/// (name, size) manifest upfront, writes the front index first, then streams each body straight to the
/// destination -- a whole pack never has to fit in memory (the point for an S3 multipart sink). Members
/// must be written in manifest order.
class PackedArchiveWriter
{
public:
    struct Member
    {
        String name; /// Member key inside the pack (the blob's data_file id, unique per member).
        UInt64 size; /// Physical byte length of the member body.
    };

    /// Writes the front index to `out_` immediately. Backup members are opaque byte ranges, so the caller
    /// passes VERSION_WITHOUT_UNCOMPRESSED_SIZE.
    PackedArchiveWriter(std::unique_ptr<WriteBuffer> out_, VectorWithMemoryTracking<Member> members_, UInt8 version_);

    ~PackedArchiveWriter();

    /// Streams one member body. Must be called once per member, in the manifest order; copies exactly
    /// the member's declared size from `in`.
    void writeMember(const String & name, ReadBuffer & in);

    /// All members must have been written. Finalizes the destination buffer.
    void finalize();

    void cancel() noexcept;

private:
    std::unique_ptr<WriteBuffer> out;
    const VectorWithMemoryTracking<Member> members;
    size_t next_member = 0;
    bool finalized = false;
};

}
