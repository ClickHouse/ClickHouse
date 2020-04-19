#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <common/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <cstdlib>

#include <bitset>
#include <limits>
#include <vector>


namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_SIZE_PARAMETER;
    }

    CompressionCodecGroupVarint::CompressionCodecGroupVarint(UInt8 data_bytes_size_)
            : data_bytes_size(data_bytes_size_)
    {
    }

    UInt8 CompressionCodecGroup::getMethodByte() const
    {
        return static_cast<UInt8>(CompressionMethodByte::GroupVarint);
    }

    String CompressionCodecGroupVarint::getCodecDesc() const
    {
        return "GroupVarint(" + toString(data_bytes_size) + ")";
    }

    namespace
    {

        /// group encode
        /// number of data in each group(1 <= n <= 4)
        void encode_single_group (vector <Uint32> & group, UInt8 group_size, UInt8 * dest)
        {
            /// the group’s tag
            UInt8 tags = 0;
            UInt8 offset = 6;

            /// total number of bytes (minus one for tags)
            UInt8 total_bytes = 0;

            /// leave 1 byte for tags
            ++dest;

            /// encode for each data
            for (size_t i = 0; i < group_size; ++i)
            {
                Uint32 number = group[i];

                /// each data has a sub-tag count record how many bytes
                Uint8 count = 0;

                /// if it is 0, it does not need to be taken, and it is directly added to dest
                if (number != 0)
                {
                    Uint8 current_byte = 0xff;

                    /// while there are still bytes that can be encoded
                    while (number != 0)
                    {
                        current_byte &= number;
                        ++count;
                        * (dest++) = current_byte;
                        number >>= 8;
                        current_byte = 0xff;
                    }
                    tags |= ((count - 1) << offset);
                }
                else
                {
                    * (dest++) = (Uint8)0x00;
                    tags = tags | (count << offset);
                }
                offset -= 2;
                total_bytes += count;
            }
            * (dest - total_bytes - 1) = tags;
        }

        /// currently just for UInt32 type of data
        void compressData(const UInt32 * source, UInt32 source_size,  UInt8 * dest)
        {
            if (!source_size)
                throw Exception("Can't encode data with zero or negative size parameter\n" , ErrorCodes::ILLEGAL_SIZE_PARAMETER);

            /// forming groups, <= 4 data in each
            Uint32 group_count = 0;
            vector <UInt32> group;

            for (int i = 0; i < source_size; ++i) {
                ++group_count;
                group.push_back( *(source + i));

                ///encode if it can fill up 4 data
                if ((group_count & 3) == 0) {
                    encode_single_group(group, group_count, dest);
                    group_count = 0;
                    group.clear();
                }
            }

            /// if the number of data is not a multiple of 4, then encode separately
            if (group_count != 0)
                encode_single_group(group, group_count, dest);
        }

        const vector <Uint32> masks = {0xff, 0xffff, 0xffffff, 0xffffffff};

        Uint32 get_value(const UInt8 * source, UInt8 current_tag, UInt8 & index)
        {
            UInt32 value = 0x0;
            UInt8 offset = 0;
            for (size_t i = 0; i < current_tag + 1; ++i) {
                value |= (* source << offset);
                offset += 8;
                ++source;
            }
            index += current_tag + 1;
            return value;
        }

        void decompressData(const UInt8 * source, UInt32 source_size, Uint32 * dest)
        {

            if (!source_size)
                throw Exception("Can't decode data with zero or negative size parameter\n" , ErrorCodes::ILLEGAL_SIZE_PARAMETER);

            /// total number of bytes including tags
            const UInt32 len = source_size;
            UInt8 index = 0;

            while (index < len)
            {
                ++index;

                /// get the current tag
                UInt8 tags = * (source++);

                /// total number of bytes in the current group
                UInt32 number = (tags & 3) + ((tags >> 2) & 3) + ((tags >> 4) & 3) + ((tags >> 6) & 3) + 4;
                UInt8 current_tag = (tags >> 6) & 3;

                /// remaining group
                if (index +  number >= len)
                {
                    UInt32 value = get_value(source, current_tag, & index); ///????? по ссылке или без
                    if(index >= len) break;

                    current_tag = (tags >> 4) & 3;
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;
                    if(index >= len) break;

                    current_tag = (tags >> 2) & 3;
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;
                    if(index >= len) break;

                    current_tag = tags & 3;
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;
                    if(index >= len) break;
                }
                else
                {
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;

                    current_tag = (tags >> 4) & 3;
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;

                    current_tag = (tags >> 2) & 3;
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;

                    current_tag = tags & 3;
                    UInt32 value = get_value(source, current_tag, & index);
                    * (dest++) = value;
                }
            }
        }
    }
}
