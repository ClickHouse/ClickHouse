#include <gtest/gtest.h>
#include <Functions/hilbertEncode.h>


void checkLookupTableConsistency(UInt8 x, UInt8 y, UInt8 state)
{
    auto step1 = DB::FunctionHilbertEncode2DWIthLookupTableImpl<1>::encodeFromState(x, y, state);
    auto step2 = DB::FunctionHilbertEncode2DWIthLookupTableImpl<3>::encodeFromState(x, y, state);
    ASSERT_EQ(step1.hilbert_code, step2.hilbert_code);
    ASSERT_EQ(step1.state, step2.state);
}


TEST(HilbertLookupTable, bitStep1And3Consistnecy)
{
    for (int x = 0; x < 8; ++x) {
        for (int y = 0; y < 8; ++y) {
            for (int state = 0; state < 4; ++state) {
                checkLookupTableConsistency(x, y, state);
            }
        }
    }
}
