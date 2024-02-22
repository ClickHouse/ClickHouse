#include <gtest/gtest.h>
#include <iostream>
#include <Functions/hilbertEncode.h>


TEST(HilbertLookupTable, bitStep1And3Consistnecy)
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
