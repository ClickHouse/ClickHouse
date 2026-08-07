#include <gtest/gtest.h>

#include <base/JSON.h>

TEST(JSON, searchField)
{
    const JSON json = JSON(std::string_view(R"({"k1":1,"k2":{"k3":2,"k4":3,"k":4},"k":5})"));
    ASSERT_EQ(json["k1"].getUInt(), 1);
    ASSERT_EQ(json["k2"].toString(), R"({"k3":2,"k4":3,"k":4})");
    ASSERT_EQ(json["k2"]["k3"].getUInt(), 2);
    ASSERT_EQ(json["k2"]["k4"].getUInt(), 3);
    ASSERT_EQ(json["k2"]["k"].getUInt(), 4);
    ASSERT_EQ(json["k"].getUInt(), 5);
}
