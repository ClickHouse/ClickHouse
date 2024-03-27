#include <gtest/gtest.h>
#include <Functions/hilbertDecode.h>
#include <Functions/hilbertEncode.h>


TEST(HilbertLookupTable, EncodeBit1And3Consistnecy)
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

TEST(HilbertLookupTable, DecodeBit1And3Consistnecy)
{
    const size_t bound = 1000 * 1000;
    for (size_t hilbert_code = 0; hilbert_code < bound; ++hilbert_code)
    {
        auto res1 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<1>::decode(hilbert_code);
        auto res3 = DB::FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(hilbert_code);
        ASSERT_EQ(res1, res3);
    }
}
