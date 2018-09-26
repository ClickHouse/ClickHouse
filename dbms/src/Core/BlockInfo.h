/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include <Core/Types.h>


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
      *  a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
      * If it is such a block, then is_overflows is set to true for it.
      */

    /** bucket_num:
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
      */

    /** is_start_frame:
     *  If stream contains blocks for multiple streams then for the first block in a new stream has is_start_frame set to true.
     */

    /** is_end_frame:
     *  If stream contains blocks for multiple streams then for the last block in the stream the is_end_frame set to true.
     */

    /** is_multiplexed:
     *  Idicates if the block belongs to a multiplexed stream which means that blocks for different tables
     *  are in the same stream
     */

    /** table:
     *  For multiplexed streams table could be set to indicate from which table the block is coming from.
     */

    /** hash:
     *  Hash could be set to indicate the hash of the whole stream or just one of its frames.
     */

#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(bool,     is_overflows,     false,     1) \
    M(Int32,    bucket_num,          -1,     2) \
    M(bool,     is_start_frame,   false,     3) \
    M(bool,     is_end_frame,     false,     4) \
    M(bool,     is_multiplexed,   false,     5) \
    M(String,   table,            "",        6) \
    M(String,   hash,             "",        7)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

    /// Write the values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
    void write(WriteBuffer & out) const;

    /// Read the values in binary form.
    void read(ReadBuffer & in);
};

}
