#include <Storages/ColumnsDescription.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnsDescription, Normalize)
{
    constexpr auto columns = "columns format version: 1\n"
                             "3 columns:\n"
                             "`a` UInt32\n"
                             "`b` String\tDEFAULT\tIf(a = 0, 'true', 'false')\n"
                             "`c` String\tDEFAULT\tcAsT(a, 'String')\n";

    constexpr auto columns_normalized = "columns format version: 1\n"
                                        "3 columns:\n"
                                        "`a` UInt32\n"
                                        "`b` String\tDEFAULT\tif(a = 0, 'true', 'false')\n"
                                        "`c` String\tDEFAULT\tcast(a, 'String')\n";

    tryRegisterFunctions();

    ASSERT_EQ(ColumnsDescription::parse(columns), ColumnsDescription::parse(columns_normalized));
}
