#pragma once

#include <Core/ProtocolDefines.h>
#include <base/types.h>

#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** More information about the block.
  */
struct BlockInfo
{
    /** is_overflows:
      * After running GROUP BY ... WITH TOTALS with the max_rows_to_group_by and group_by_overflow_mode = 'any' settings,
      * a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
      * If it is such a block, then is_overflows is set to true for it.
      */

    /** bucket_num:
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
      */

    /** out_of_order_buckets:
      * List of id-s of buckets delayed by `ConvertingAggregatedToChunksTransform` on the current node.
      * Please refer to the comment in `ConvertingAggregatedToChunksTransform` for more details.
      */

#define APPLY_FOR_BLOCK_INFO_FIELDS(M)                                                                                \
    M(bool,               is_overflows,         false, 1, 0)                                                          \
    M(Int32,              bucket_num,           -1,    2, 0)                                                          \
    M(std::vector<Int32>, out_of_order_buckets, {},    3, DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM, MIN_PROTOCOL_REVISION) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

    /// Write the values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;

    /// Read the values in binary form.
    void read(ReadBuffer & in, UInt64 client_protocol_revision);
};

}
