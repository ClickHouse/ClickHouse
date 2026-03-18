#include <gtest/gtest.h>
#include "Functions/hilbertDecode2DLUT.h"
#include "Functions/hilbertEncode2DLUT.h"
#include "base/types.h"


TEST(HilbertLookupTable, EncodeBit1And3Consistency)
{
    const size_t bound = 1000;
    for (size_t x = 0; x < bound; ++x)
    {
        for (size_t y = 0; y < bound; ++y)
        {
            auto hilbert1bit = DB::FunctionHilbertEncode2DWIthLookupTableImpl<1>::encode(x, y);
            auto hilbert3bit = DB::FunctionHilbertEncode2DWIthLookupTableImpl<3>::encode(x, y);
            ASSERT_EQ(hilbert1bit, hilbert3bit);
        }
    }
}

TEST(HilbertLookupTable, EncodeBit2And3Consistency)
{
    const size_t bound = 1000;
    for (size_t x = 0; x < bound; ++x)
    {
        for (size_t y = 0; y < bound; ++y)
        {
            auto hilbert2bit = DB::FunctionHilbertEncode2DWIthLookupTableImpl<2>::encode(x, y);
            auto hilbert3bit = DB::FunctionHilbertEncode2DWIthLookupTableImpl<3>::encode(x, y);
            ASSERT_EQ(hilbert3bit, hilbert2bit);
        }
    }
}

TEST(HilbertLookupTable, DecodeBit1And3Consistency)
{
    const size_t bound = 1000 * 1000;
    for (size_t hilbert_code = 0; hilbert_code < bound; ++hilbert_code)
    {
        auto res1 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<1>::decode(hilbert_code);
        auto res3 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(hilbert_code);
        ASSERT_EQ(res1, res3);
    }
}

TEST(HilbertLookupTable, DecodeBit2And3Consistency)
{
    const size_t bound = 1000 * 1000;
    for (size_t hilbert_code = 0; hilbert_code < bound; ++hilbert_code)
    {
        auto res2 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<2>::decode(hilbert_code);
        auto res3 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(hilbert_code);
        ASSERT_EQ(res2, res3);
    }
}

TEST(HilbertLookupTable, DecodeAndEncodeAreInverseOperations)
{
    const size_t bound = 1000;
    for (size_t x = 0; x < bound; ++x)
    {
        for (size_t y = 0; y < bound; ++y)
        {
            auto hilbert_code = DB::FunctionHilbertEncode2DWIthLookupTableImpl<3>::encode(x, y);
            auto [x_new, y_new] = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(hilbert_code);
            ASSERT_EQ(x_new, x);
            ASSERT_EQ(y_new, y);
        }
    }
}

TEST(HilbertLookupTable, EncodeAndDecodeAreInverseOperations)
{
    const size_t bound = 1000 * 1000;
    for (size_t hilbert_code = 0; hilbert_code < bound; ++hilbert_code)
    {
        auto [x, y] = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(hilbert_code);
        auto hilbert_new = DB::FunctionHilbertEncode2DWIthLookupTableImpl<3>::encode(x, y);
        ASSERT_EQ(hilbert_new, hilbert_code);
    }
}
