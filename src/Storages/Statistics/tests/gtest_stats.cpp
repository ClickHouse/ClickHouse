#include <gtest/gtest.h>

#include <Storages/Statistics/StatisticsTDigest.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/DataTypeFactory.h>

using namespace DB;

TEST(Statistics, TDigestLessThan)
{
    /// this is the simplest data which is continuous integeters.
    /// so the estimated errors should be low.

    std::vector<Int64> data;
    data.reserve(100000);
    for (int i = 0; i < 100000; i++)
        data.push_back(i);

    auto test_less_than = [](const std::vector<Int64> & data1,
                             const std::vector<double> & v,
                             const std::vector<double> & answers,
                             const std::vector<double> & eps)
    {

        DB::QuantileTDigest<Int64> t_digest;

        for (Int64 i : data1)
            t_digest.add(i);

        t_digest.compress();

        for (int i = 0; i < v.size(); i ++)
        {
            auto value = v[i];
            auto result = t_digest.getCountLessThan(value);
            auto answer = answers[i];
            auto error = eps[i];
            ASSERT_LE(result, answer * (1 + error));
            ASSERT_GE(result, answer * (1 - error));
        }
    };
    test_less_than(data, {-1, 1e9, 50000.0, 3000.0, 30.0}, {0, 100000, 50000, 3000, 30}, {0, 0, 0.001, 0.001, 0.001});

    std::reverse(data.begin(), data.end());
    test_less_than(data, {-1, 1e9, 50000.0, 3000.0, 30.0}, {0, 100000, 50000, 3000, 30}, {0, 0, 0.001, 0.001, 0.001});
}

using Fields = std::vector<Field>;

template <typename T>
void testConvertFieldToDataType(const DataTypePtr & data_type, const Fields & fields, const T & expected_value, bool convert_failed = false)
{
    for (const auto & field : fields)
    {
        Field converted_value;
        try
        {
            converted_value = convertFieldToType(field, *data_type);
        }
        catch(...)
        {
            ASSERT_FALSE(convert_failed);
        }
        if (convert_failed)
            ASSERT_TRUE(converted_value.isNull());
        else
            ASSERT_EQ(converted_value.template get<T>(), expected_value);
    }
}

TEST(Statistics, convertFieldToType)
{
    Fields fields;

    auto data_type_int8 = DataTypeFactory::instance().get("Int8");
    fields = {1, 1.0, "1"};
    testConvertFieldToDataType(data_type_int8, fields, static_cast<Int8>(1));

    fields = {256, 1.1, "not a number"};
    testConvertFieldToDataType(data_type_int8, fields, static_cast<Int8>(1), true);

    auto data_type_float64 = DataTypeFactory::instance().get("Float64");
    fields = {1, 1.0, "1.0"};
    testConvertFieldToDataType(data_type_float64, fields, static_cast<Float64>(1.0));

    auto data_type_string = DataTypeFactory::instance().get("String");
    fields = {1, "1"};
    testConvertFieldToDataType(data_type_string, fields, static_cast<String>("1"));

    auto data_type_date = DataTypeFactory::instance().get("Date");
    fields = {"2024-07-12", 19916};
    testConvertFieldToDataType(data_type_date, fields, static_cast<UInt64>(19916));
}
