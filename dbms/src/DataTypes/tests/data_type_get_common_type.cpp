#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastCommonType.h>
#include <DataTypes/getMostCommonType.h>

#include <sstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>

#pragma GCC diagnostic pop

using namespace DB;


TEST(data_type, data_type_get_common_type_Test)
{
    try
    {
        auto & data_type_factory = DataTypeFactory::instance();
        auto typeFromString = [& data_type_factory](const std::string & str)
        {
            return data_type_factory.get(str);
        };

        auto typesFromString = [& typeFromString](const std::string & str)
        {
            std::istringstream data_types_stream(str);
            DataTypes data_types;
            std::string data_type;
            while (data_types_stream >> data_type)
                data_types.push_back(typeFromString(data_type));

            return data_types;
        };

        ASSERT_TRUE(getLeastCommonType(typesFromString(""))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Nothing"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("UInt8 UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Int8 Int8"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("UInt8 Int8"))->equals(*typeFromString("Int16")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("UInt8 Int16"))->equals(*typeFromString("Int16")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("UInt8 UInt32 UInt64"))->equals(*typeFromString("UInt64")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Int8 Int32 Int64"))->equals(*typeFromString("Int64")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("UInt8 UInt32 Int64"))->equals(*typeFromString("Int64")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("Float32 Float64"))->equals(*typeFromString("Float64")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Float32 UInt16 Int16"))->equals(*typeFromString("Float32")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Float32 UInt16 Int32"))->equals(*typeFromString("Float64")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Float32 Int16 UInt32"))->equals(*typeFromString("Float64")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("Date Date"))->equals(*typeFromString("Date")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Date DateTime"))->equals(*typeFromString("DateTime")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("String FixedString(32) FixedString(8)"))->equals(*typeFromString("String")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(UInt8) Array(UInt8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(UInt8) Array(Int8)"))->equals(*typeFromString("Array(Int16)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(Float32) Array(Int16) Array(UInt32)"))->equals(*typeFromString("Array(Float64)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(Array(UInt8)) Array(Array(UInt8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(Array(UInt8)) Array(Array(Int8))"))->equals(*typeFromString("Array(Array(Int16))")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(Date) Array(DateTime)"))->equals(*typeFromString("Array(DateTime)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(String)")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("Nullable(Nothing) Nothing"))->equals(*typeFromString("Nullable(Nothing)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Nullable(UInt8) Int8"))->equals(*typeFromString("Nullable(Int16)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Nullable(Nothing) UInt8 Int8"))->equals(*typeFromString("Nullable(Int16)")));

        ASSERT_TRUE(getLeastCommonType(typesFromString("Tuple(Int8,UInt8) Tuple(UInt8,Int8)"))->equals(*typeFromString("Tuple(Int16,Int16)")));
        ASSERT_TRUE(getLeastCommonType(typesFromString("Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))"))->equals(*typeFromString("Tuple(Nullable(UInt8))")));

        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Int8 String")));
        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Int64 UInt64")));
        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Float32 UInt64")));
        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Float64 Int64")));
        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Tuple(Int64) Tuple(UInt64)")));
        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Tuple(Int64, Int8) Tuple(UInt64)")));
        EXPECT_ANY_THROW(getLeastCommonType(typesFromString("Array(Int64) Array(String)")));


        ASSERT_TRUE(getMostCommonType(typesFromString(""))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Nothing"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getMostCommonType(typesFromString("UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("UInt8 UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Int8 Int8"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("UInt8 Int8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Int8 UInt16"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("UInt8 UInt32 UInt64"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Int8 Int32 Int64"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("UInt8 Int64 UInt64"))->equals(*typeFromString("UInt8")));

        ASSERT_TRUE(getMostCommonType(typesFromString("Float32 Float64"))->equals(*typeFromString("Float32")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Float32 UInt16 Int16"))->equals(*typeFromString("UInt16")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Float32 UInt16 Int32"))->equals(*typeFromString("UInt16")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Float32 Int16 UInt32"))->equals(*typeFromString("Int16")));

        ASSERT_TRUE(getMostCommonType(typesFromString("DateTime DateTime"))->equals(*typeFromString("DateTime")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Date DateTime"))->equals(*typeFromString("Date")));

        ASSERT_TRUE(getMostCommonType(typesFromString("String FixedString(8)"))->equals(*typeFromString("FixedString(8)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("FixedString(16) FixedString(8)"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getMostCommonType(typesFromString("Array(UInt8) Array(UInt8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(UInt8) Array(Int8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(Float32) Array(Int16) Array(UInt32)"))->equals(*typeFromString("Array(Int16)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(Array(UInt8)) Array(Array(UInt8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(Array(UInt8)) Array(Array(Int8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(Date) Array(DateTime)"))->equals(*typeFromString("Array(Date)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(FixedString(32))")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(FixedString(32))")));

        ASSERT_TRUE(getMostCommonType(typesFromString("Nullable(Nothing) Nothing"))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Nullable(UInt8) Int8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Nullable(Nothing) UInt8 Int8"))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Nullable(UInt8) Nullable(Int8)"))->equals(*typeFromString("Nullable(UInt8)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Nullable(Nothing) Nullable(Int8)"))->equals(*typeFromString("Nullable(Nothing)")));

        ASSERT_TRUE(getMostCommonType(typesFromString("Tuple(Int8,UInt8) Tuple(UInt8,Int8)"))->equals(*typeFromString("Tuple(UInt8,UInt8)")));
        ASSERT_TRUE(getMostCommonType(typesFromString("Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))"))->equals(*typeFromString("Tuple(Nullable(Nothing))")));

        EXPECT_ANY_THROW(getMostCommonType(typesFromString("Int8 String"), true));
        EXPECT_ANY_THROW(getMostCommonType(typesFromString("Nothing"), true));
        EXPECT_ANY_THROW(getMostCommonType(typesFromString("FixedString(16) FixedString(8) String"), true));

    }
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        bool print_stack_trace = true;

        auto embedded_stack_trace_pos = text.find("Stack trace");
        if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
            text.resize(embedded_stack_trace_pos);

        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

        if (print_stack_trace && std::string::npos == embedded_stack_trace_pos)
        {
            std::cerr << "Stack trace:" << std::endl
                      << e.getStackTrace().toString();
        }

        throw;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
        throw;
    }
    catch (const std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        throw;
    }
    catch (...)
    {
        std::cerr << "Unknown exception" << std::endl;
        throw;
    }
}
