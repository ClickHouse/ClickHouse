#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>

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

        ASSERT_TRUE(getLeastSupertype(typesFromString(""))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Nothing"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Int8 Int8"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 Int8"))->equals(*typeFromString("Int16")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 Int16"))->equals(*typeFromString("Int16")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 UInt32 UInt64"))->equals(*typeFromString("UInt64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Int8 Int32 Int64"))->equals(*typeFromString("Int64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("UInt8 UInt32 Int64"))->equals(*typeFromString("Int64")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 Float64"))->equals(*typeFromString("Float64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 UInt16 Int16"))->equals(*typeFromString("Float32")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 UInt16 Int32"))->equals(*typeFromString("Float64")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Float32 Int16 UInt32"))->equals(*typeFromString("Float64")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Date Date"))->equals(*typeFromString("Date")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Date DateTime"))->equals(*typeFromString("DateTime")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("String FixedString(32) FixedString(8)"))->equals(*typeFromString("String")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(UInt8) Array(UInt8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(UInt8) Array(Int8)"))->equals(*typeFromString("Array(Int16)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Float32) Array(Int16) Array(UInt32)"))->equals(*typeFromString("Array(Float64)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Array(UInt8)) Array(Array(UInt8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Array(UInt8)) Array(Array(Int8))"))->equals(*typeFromString("Array(Array(Int16))")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(Date) Array(DateTime)"))->equals(*typeFromString("Array(DateTime)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(String)")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Nullable(Nothing) Nothing"))->equals(*typeFromString("Nullable(Nothing)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Nullable(UInt8) Int8"))->equals(*typeFromString("Nullable(Int16)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Nullable(Nothing) UInt8 Int8"))->equals(*typeFromString("Nullable(Int16)")));

        ASSERT_TRUE(getLeastSupertype(typesFromString("Tuple(Int8,UInt8) Tuple(UInt8,Int8)"))->equals(*typeFromString("Tuple(Int16,Int16)")));
        ASSERT_TRUE(getLeastSupertype(typesFromString("Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))"))->equals(*typeFromString("Tuple(Nullable(UInt8))")));

        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Int8 String")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Int64 UInt64")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Float32 UInt64")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Float64 Int64")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Tuple(Int64) Tuple(UInt64)")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Tuple(Int64, Int8) Tuple(UInt64)")));
        EXPECT_ANY_THROW(getLeastSupertype(typesFromString("Array(Int64) Array(String)")));


        ASSERT_TRUE(getMostSubtype(typesFromString(""))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nothing"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 UInt8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Int8 Int8"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 Int8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Int8 UInt16"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 UInt32 UInt64"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Int8 Int32 Int64"))->equals(*typeFromString("Int8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("UInt8 Int64 UInt64"))->equals(*typeFromString("UInt8")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 Float64"))->equals(*typeFromString("Float32")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 UInt16 Int16"))->equals(*typeFromString("UInt16")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 UInt16 Int32"))->equals(*typeFromString("UInt16")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Float32 Int16 UInt32"))->equals(*typeFromString("Int16")));

        ASSERT_TRUE(getMostSubtype(typesFromString("DateTime DateTime"))->equals(*typeFromString("DateTime")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Date DateTime"))->equals(*typeFromString("Date")));

        ASSERT_TRUE(getMostSubtype(typesFromString("String FixedString(8)"))->equals(*typeFromString("FixedString(8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("FixedString(16) FixedString(8)"))->equals(*typeFromString("Nothing")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Array(UInt8) Array(UInt8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(UInt8) Array(Int8)"))->equals(*typeFromString("Array(UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Float32) Array(Int16) Array(UInt32)"))->equals(*typeFromString("Array(Int16)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Array(UInt8)) Array(Array(UInt8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Array(UInt8)) Array(Array(Int8))"))->equals(*typeFromString("Array(Array(UInt8))")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(Date) Array(DateTime)"))->equals(*typeFromString("Array(Date)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(FixedString(32))")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Array(String) Array(FixedString(32))"))->equals(*typeFromString("Array(FixedString(32))")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(Nothing) Nothing"))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(UInt8) Int8"))->equals(*typeFromString("UInt8")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(Nothing) UInt8 Int8"))->equals(*typeFromString("Nothing")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(UInt8) Nullable(Int8)"))->equals(*typeFromString("Nullable(UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Nullable(Nothing) Nullable(Int8)"))->equals(*typeFromString("Nullable(Nothing)")));

        ASSERT_TRUE(getMostSubtype(typesFromString("Tuple(Int8,UInt8) Tuple(UInt8,Int8)"))->equals(*typeFromString("Tuple(UInt8,UInt8)")));
        ASSERT_TRUE(getMostSubtype(typesFromString("Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))"))->equals(*typeFromString("Tuple(Nullable(Nothing))")));

        EXPECT_ANY_THROW(getMostSubtype(typesFromString("Int8 String"), true));
        EXPECT_ANY_THROW(getMostSubtype(typesFromString("Nothing"), true));
        EXPECT_ANY_THROW(getMostSubtype(typesFromString("FixedString(16) FixedString(8) String"), true));

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
