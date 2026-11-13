#include "config.h"

#if USE_ICU

#include <gtest/gtest.h>

#include <Columns/Collator.h>
#include <Common/Exception.h>

#include <cstdint>
#include <limits>

namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
}

TEST(Collator, RejectsLengthAboveInt32Max)
{
    Collator collator("en");
    /// The guard throws before ICU reads the string, so a short buffer is enough.
    const char * str = "a";
    constexpr size_t max_length = std::numeric_limits<int32_t>::max();

    for (size_t length : {max_length + 1, size_t{std::numeric_limits<uint32_t>::max()}, (size_t{1} << 32) + 1})
    {
        for (bool first : {true, false})
        {
            try
            {
                if (first)
                    collator.compare(str, length, str, 1);
                else
                    collator.compare(str, 1, str, length);
                ADD_FAILURE() << "length " << length << " (operand " << (first ? 1 : 2) << ") was not rejected";
            }
            catch (const DB::Exception & e)
            {
                ASSERT_EQ(e.code(), DB::ErrorCodes::TOO_LARGE_STRING_SIZE);
            }
        }
    }
}

#endif
