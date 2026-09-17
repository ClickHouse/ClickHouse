#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Common/Exception.h>

#include <IO/WriteBuffer.h>

#include <array>
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::UDT
{
namespace
{

/// Streams the complete binary type encoding into SHA-256. Keeping the buffer
/// on the caller's stack avoids materializing and copying an O(type bytes)
/// temporary String during schema admission.
class CanonicalHashWriteBuffer final : public WriteBuffer
{
public:
    CanonicalHashWriteBuffer(char * storage, std::size_t size, CanonicalHasher & hash_)
        : WriteBuffer(storage, size)
        , hash(hash_)
    {
    }

    ~CanonicalHashWriteBuffer() override
    {
        if (!isFinalized() && !isCanceled())
            cancel();
    }

private:
    void nextImpl() override
    {
        hash.update(std::span<const CanonicalByte>(reinterpret_cast<const CanonicalByte *>(working_buffer.begin()), offset()));
    }

    CanonicalHasher & hash;
};

}

Digest physicalTypeFingerprint(const DataTypePtr & type)
{
    if (!type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot fingerprint a null physical data type");

    CanonicalHasher hash(physical_type_fingerprint_domain);
    std::array<char, 4096> storage;
    CanonicalHashWriteBuffer buffer(storage.data(), storage.size(), hash);
    encodeCanonicalDataType(type, buffer);
    buffer.finalize();
    return hash.finalize();
}

}
