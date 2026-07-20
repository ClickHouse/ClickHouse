#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>

using namespace DB;

namespace
{

struct TypedArgument
{
    DataTypePtr type;
    Field value;
};

FunctionBasePtr buildFunction(const String & name, const DataTypes & argument_types)
{
    tryRegisterFunctions();

    auto context = getContext().context;
    auto resolver = FunctionFactory::instance().get(name, context);

    ColumnsWithTypeAndName arguments;
    arguments.reserve(argument_types.size());
    for (const auto & argument_type : argument_types)
        arguments.emplace_back(ColumnWithTypeAndName{nullptr, argument_type, ""});

    return resolver->build(arguments);
}

Field evaluateFunction(const String & name, const std::vector<TypedArgument> & arguments)
{
    DataTypes argument_types;
    for (const auto & arg : arguments)
        argument_types.push_back(arg.type);

    auto function = buildFunction(name, argument_types);

    ColumnsWithTypeAndName args;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto & arg = arguments[i];
        auto column = arg.type->createColumn();
        column->insert(arg.value);

        // For constant arguments (index 1 for port functions), wrap in ColumnConst
        if (i == 1)
        {
            auto const_column = ColumnConst::create(column->convertToFullColumnIfLowCardinality(), 1);
            args.emplace_back(ColumnWithTypeAndName{std::move(const_column), arg.type, ""});
        }
        else
        {
            args.emplace_back(ColumnWithTypeAndName{std::move(column), arg.type, ""});
        }
    }

    chassert(!args.empty());
    auto result = function->execute(args, std::make_shared<DataTypeUInt16>(), args[0].column->size(), false);
    return (*result)[0];
}

/// Overload for backward compatibility with single-argument string tests
Field evaluateFunction(const String & name, const std::vector<String> & arguments_str)
{
    std::vector<TypedArgument> arguments;
    for (const auto & str : arguments_str)
        arguments.push_back({std::make_shared<DataTypeString>(), str});
    return evaluateFunction(name, arguments);
}

}

TEST(PortFunction, UserinfoWithoutPort)
{
    // Test cases for URLs with userinfo but no port

    // URL: //paul@www.example.com - no port, should return 0
    EXPECT_EQ(evaluateFunction("port", {"//paul@www.example.com"}), UInt64(0));

    // URL: //user@host - no port, should return 0
    EXPECT_EQ(evaluateFunction("port", {"//user@host"}), UInt64(0));

    // URL: //@example.com - empty userinfo, no port
    EXPECT_EQ(evaluateFunction("port", {"//@example.com"}), UInt64(0));

    // URL: http://user@example.com - scheme + userinfo, no port (regression for sanitizer report)
    EXPECT_EQ(evaluateFunction("port", {"http://user@example.com"}), UInt64(0));

    // URL: https://user@example.com - scheme + userinfo, no port
    EXPECT_EQ(evaluateFunction("port", {"https://user@example.com"}), UInt64(0));
}

TEST(PortFunction, UserinfoWithPort)
{
    // Test cases for URLs with userinfo AND port

    // URL: //user@example.com:8080
    EXPECT_EQ(evaluateFunction("port", {"//user@example.com:8080"}), UInt64(8080));

    // URL: //paul@www.example.com:8080
    EXPECT_EQ(evaluateFunction("port", {"//paul@www.example.com:8080"}), UInt64(8080));
}

TEST(PortFunction, NoUserinfo)
{
    // Test cases without userinfo

    // URL: //example.com - no port
    EXPECT_EQ(evaluateFunction("port", {"//example.com"}), UInt64(0));

    // URL: //www.example.com:9090
    EXPECT_EQ(evaluateFunction("port", {"//www.example.com:9090"}), UInt64(9090));

    // URL with scheme: https://example.com:443
    EXPECT_EQ(evaluateFunction("port", {"https://example.com:443"}), UInt64(443));
}

TEST(PortFunction, EdgeCases)
{
    // Empty URL - should return 0
    EXPECT_EQ(evaluateFunction("port", {""}), UInt64(0));

    // URL with just scheme separator
    EXPECT_EQ(evaluateFunction("port", {"//"}), UInt64(0));

    // URL with path but no host or port
    EXPECT_EQ(evaluateFunction("port", {"//host/path"}), UInt64(0));
}

TEST(PortFunction, RFCVersion)
{
    // Test portRFC function with userinfo URLs

    // URL: //user@example.com - no port
    EXPECT_EQ(evaluateFunction("portRFC", {"//user@example.com"}), UInt64(0));

    // URL: //paul@www.example.com:8080
    EXPECT_EQ(evaluateFunction("portRFC", {"//paul@www.example.com:8080"}), UInt64(8080));

    // URL: http://user@example.com - scheme + userinfo, no port (regression for sanitizer report)
    EXPECT_EQ(evaluateFunction("portRFC", {"http://user@example.com"}), UInt64(0));

    // URL: https://user@example.com - scheme + userinfo, no port
    EXPECT_EQ(evaluateFunction("portRFC", {"https://user@example.com"}), UInt64(0));
}

TEST(PortFunction, TwoArgumentWithDefault)
{
    // Test two-argument form: port(url, default_port)
    // URL with explicit port should return that port, not default_port combined with digits

    // The original bug case: http://example.com:80 with default 443
    // Should return 80, NOT 44380
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com:80"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(80));

    // Another explicit port
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com:8080"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(8080));

    // URL with no port should return default
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // URL with empty port suffix should return default
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com:"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // URL with non-digit suffix should return default
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com:abc"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // URL with scheme + userinfo, no port: should return default (regression for sanitizer report)
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://user@example.com"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // URL with scheme + userinfo + explicit port: should return that port
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://user@example.com:8080"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(8080));
}

TEST(PortFunction, TwoArgumentOverflow)
{
    // Port number overflow - 70000 is larger than UInt16 max (65535)
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com:70000"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // Very large port
    EXPECT_EQ(evaluateFunction("port", {{std::make_shared<DataTypeString>(), "http://example.com:99999"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));
}

TEST(PortFunction, TwoArgumentWithRFC)
{
    // Test two-argument form for portRFC

    // Explicit port with default
    EXPECT_EQ(evaluateFunction("portRFC", {{std::make_shared<DataTypeString>(), "http://example.com:80"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(80));

    // URL with no port should return default
    EXPECT_EQ(evaluateFunction("portRFC", {{std::make_shared<DataTypeString>(), "http://example.com"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // Empty port suffix should return default
    EXPECT_EQ(evaluateFunction("portRFC", {{std::make_shared<DataTypeString>(), "http://example.com:"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // Non-digit suffix should return default
    EXPECT_EQ(evaluateFunction("portRFC", {{std::make_shared<DataTypeString>(), "http://example.com:xyz"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // URL with scheme + userinfo, no port: should return default (regression for sanitizer report)
    EXPECT_EQ(evaluateFunction("portRFC", {{std::make_shared<DataTypeString>(), "http://user@example.com"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(443));

    // URL with scheme + userinfo + explicit port: should return that port
    EXPECT_EQ(evaluateFunction("portRFC", {{std::make_shared<DataTypeString>(), "http://user@example.com:8080"}, {std::make_shared<DataTypeUInt16>(), UInt64(443)}}), UInt64(8080));
}
