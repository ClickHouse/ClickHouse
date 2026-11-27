#include <Core/ProtocolDefines.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/BlockInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_BLOCK_INFO_FIELD;
    extern const int INCORRECT_DATA;
}


/// Write values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
void BlockInfo::write(WriteBuffer & out, UInt64 server_protocol_revision) const
{
/// Set of pairs `FIELD_NUM`, value in binary form. Then 0.
#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM, MIN_PROTOCOL_REVISION) \
    if (server_protocol_revision >= (MIN_PROTOCOL_REVISION)) \
    { \
        writeVarUInt(FIELD_NUM, out); \
        writeBinary(NAME, out); \
    }

    APPLY_FOR_BLOCK_INFO_FIELDS(WRITE_FIELD)

#undef WRITE_FIELD
    writeVarUInt(0, out);
}

namespace
{

template <typename T>
void readField(T & value, ReadBuffer & in)
{
    readBinary(value, in);
}

/** The number of out of order buckets comes from the peer, and the vector is resized to it before the
  * bucket ids are read. A list of two-level aggregation bucket ids cannot name more buckets than
  * there are (256), and in practice it holds a few of them.
  */
constexpr size_t MAX_OUT_OF_ORDER_BUCKETS = 256;

/// Bucket numbers are used as indices of the buckets of two-level aggregation (there are 256 of them).
constexpr Int32 MAX_BUCKET_NUM = 255;

void checkBucketNum(Int32 bucket_num, Int32 min_bucket_num)
{
    if (bucket_num < min_bucket_num || bucket_num > MAX_BUCKET_NUM)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Bucket number {} in BlockInfo is out of range, expected a value from {} to {}",
            bucket_num, min_bucket_num, MAX_BUCKET_NUM);
}

/// The only `Int32` field is `bucket_num`, which is -1 for a block that does not belong to a bucket.
void readField(Int32 & value, ReadBuffer & in)
{
    readBinary(value, in);
    checkBucketNum(value, -1);
}

void readField(VectorWithMemoryTracking<Int32> & value, ReadBuffer & in)
{
    readVectorBinary(value, in, MAX_OUT_OF_ORDER_BUCKETS);
    for (Int32 bucket : value)
        checkBucketNum(bucket, 0);
}

}

/// Read values in binary form.
void BlockInfo::read(ReadBuffer & in, UInt64 client_protocol_revision)
{
    UInt64 field_num = 0;

    while (true)
    {
        readVarUInt(field_num, in);
        if (field_num == 0)
            break;

        switch (field_num)
        {
#define READ_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM, MIN_PROTOCOL_REVISION) \
    case FIELD_NUM: \
        if (client_protocol_revision >= (MIN_PROTOCOL_REVISION)) \
            readField(NAME, in); \
        break;

            APPLY_FOR_BLOCK_INFO_FIELDS(READ_FIELD)

        #undef READ_FIELD
            default:
                throw Exception(ErrorCodes::UNKNOWN_BLOCK_INFO_FIELD, "Unknown BlockInfo field number: {}", field_num);
        }
    }
}

}
