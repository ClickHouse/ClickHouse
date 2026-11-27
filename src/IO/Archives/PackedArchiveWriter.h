#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
#include <memory>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Streaming writer for the ".packed" format: takes the ordered (name, size) manifest upfront and streams each body,
/// so a pack need not fit in memory. Members are written in manifest order.
class PackedArchiveWriter
{
public:
    struct Member
    {
        String name; /// Member key inside the pack (unique per pack).
        UInt64 size; /// Physical byte length of the member body.
    };

    /// Writes the front index to `out_` immediately.
    PackedArchiveWriter(std::unique_ptr<WriteBuffer> out_, VectorWithMemoryTracking<Member> members_, UInt8 version_);

    ~PackedArchiveWriter();

    /// Streams one member body (exactly its declared size), in manifest order.
    void writeMember(const String & name, ReadBuffer & in);

    /// Finalizes the destination buffer.
    void finalize();

private:
    std::unique_ptr<WriteBuffer> out;
    const VectorWithMemoryTracking<Member> members;
    size_t next_member = 0;
};

}
