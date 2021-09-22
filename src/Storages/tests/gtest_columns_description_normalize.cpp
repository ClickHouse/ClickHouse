#include <Storages/ColumnsDescription.h>
#include <Functions/registerFunctions.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnsDescription, Normalize)
{
    constexpr auto columns = R"(columns format version: 1
3 columns:
`a` UInt32
`b` String	DEFAULT	If(a = 0, \'true\', \'false\')
`c` String	DEFAULT	cAsT(a, 'String')
)";

    constexpr auto columns_normalized = R"(columns format version: 1
3 columns:
`a` UInt32
`b` String	DEFAULT	if(a = 0, \'true\', \'false\')
`c` String	DEFAULT	cast(a, 'String')
)";

    registerFunctions();

    ASSERT_EQ(ColumnsDescription::parse(columns), ColumnsDescription::parse(columns_normalized));
}
