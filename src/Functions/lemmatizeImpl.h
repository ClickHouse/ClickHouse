#pragma once
#include <Columns/ColumnString.h>
//#include <Poco/UTF8Encoding.h>
#include <Common/UTF8Helpers.h>
#include <sphinxstd.h>
#include <sphinxstemen.cpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct LemmatizeImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        
        UInt64 data_size = 0;
        for (UInt64 i = 0; i < offsets.size(); ++i)
        {
            /// Note that accessing -1th element is valid for PaddedPODArray.
            size_t original_size = offsets[i] - offsets[i - 1];
            memcpy(res_data.data() + data_size, data.data() + offsets[i - 1], original_size);
            
            stemImpl(res_data.data() + data_size, original_size - 1);
            
            UInt64 new_size = length(res_data.data() + data_size);
            data_size += new_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
    }

    static UInt64 length(const UInt8 * src) {
        UInt64 res = 0;
        while (*src++ != 0) {
            res++;
        }
        return res + 1;
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Function lemmatize cannot work with FixedString argument", ErrorCodes::BAD_ARGUMENTS);
    }


private:

    static void stemImpl(UInt8 * dst, UInt64 length) {
        stem_en(reinterpret_cast<unsigned char *>(dst), length);
    }

};

}
