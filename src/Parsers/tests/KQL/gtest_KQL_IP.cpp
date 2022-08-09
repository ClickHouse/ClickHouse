#include <Parsers/tests/gtest_common.h>

#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

class ParserRegexTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
{};

TEST_P(ParserRegexTest, parseQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_TRUE(parser);
    ASSERT_TRUE(expected_ast);

    DB::ASTPtr ast;
    ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
    EXPECT_THAT(serializeAST(*ast->clone(), false), ::testing::MatchesRegex(expected_ast));
}

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserRegexTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print format_ipv4(A)",
            "SELECT ifNull\\(if\\(\\(\\(\\(toUInt32OrNull\\(toString\\(A\\)\\) AS param_as_uint32_\\d+\\) IS NOT NULL\\) AND \\(toTypeName\\(A\\) = 'String'\\)\\) OR \\(32 < 0\\) OR \\(\\(ifNull\\(param_as_uint32_\\d+, multiIf\\(length\\(splitByChar\\('/', ifNull\\(toString\\(A\\), ''\\)\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\)\\) AS ip_as_number_\\d+\\) IS NULL\\), NULL, IPv4NumToString\\(bitAnd\\(ip_as_number_\\d+, bitNot\\(toUInt32\\(intExp2\\(32 - 32\\) - 1\\)\\)\\)\\)\\), ''\\)"
        },
        {
            "print format_ipv4(A, B)",
            "SELECT ifNull\\(if\\(\\(\\(\\(toUInt32OrNull\\(toString\\(A\\)\\) AS param_as_uint32_\\d+\\) IS NOT NULL\\) AND \\(toTypeName\\(A\\) = 'String'\\)\\) OR \\(B < 0\\) OR \\(\\(ifNull\\(param_as_uint32_\\d+, multiIf\\(length\\(splitByChar\\('/', ifNull\\(toString\\(A\\), ''\\)\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\)\\) AS ip_as_number_\\d+\\) IS NULL\\), NULL, IPv4NumToString\\(bitAnd\\(ip_as_number_\\d+, bitNot\\(toUInt32\\(intExp2\\(32 - B\\) - 1\\)\\)\\)\\)\\), ''\\)"
        },
        {
            "print format_ipv4_mask(A)",
            "SELECT if\\(empty\\(ifNull\\(if\\(\\(\\(\\(toUInt32OrNull\\(toString\\(A\\)\\) AS param_as_uint32_\\d+\\) IS NOT NULL\\) AND \\(toTypeName\\(A\\) = 'String'\\)\\) OR \\(32 < 0\\) OR \\(\\(ifNull\\(param_as_uint32_\\d+, multiIf\\(length\\(splitByChar\\('/', ifNull\\(toString\\(A\\), ''\\)\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\)\\) AS ip_as_number_\\d+\\) IS NULL\\), NULL, IPv4NumToString\\(bitAnd\\(ip_as_number_\\d+, bitNot\\(toUInt32\\(intExp2\\(32 - 32\\) - 1\\)\\)\\)\\)\\), ''\\) AS formatted_ip_\\d+\\) OR \\(NOT \\(\\(32 >= 0\\) AND \\(32 <= 32\\)\\)\\), '', concat\\(formatted_ip_\\d+, '/', toString\\(32\\)\\)\\)"
        },
        {
            "print format_ipv4_mask(A, B)",
            "SELECT if\\(empty\\(ifNull\\(if\\(\\(\\(\\(toUInt32OrNull\\(toString\\(A\\)\\) AS param_as_uint32_\\d+\\) IS NOT NULL\\) AND \\(toTypeName\\(A\\) = 'String'\\)\\) OR \\(B < 0\\) OR \\(\\(ifNull\\(param_as_uint32_\\d+, multiIf\\(length\\(splitByChar\\('/', ifNull\\(toString\\(A\\), ''\\)\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\)\\) AS ip_as_number_\\d+\\) IS NULL\\), NULL, IPv4NumToString\\(bitAnd\\(ip_as_number_\\d+, bitNot\\(toUInt32\\(intExp2\\(32 - B\\) - 1\\)\\)\\)\\)\\), ''\\) AS formatted_ip_\\d+\\) OR \\(NOT \\(\\(B >= 0\\) AND \\(B <= 32\\)\\)\\), '', concat\\(formatted_ip_\\d+, '/', toString\\(B\\)\\)\\)"
        },
        {
            "print ipv4_compare(A, B)",
            "SELECT if\\(\\(\\(multiIf\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS lhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS lhs_mask_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS rhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS rhs_mask_\\d+\\) IS NULL\\), NULL, sign\\(toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(lhs_ip_\\d+\\), toUInt8\\(min2\\(32, min2\\(assumeNotNull\\(lhs_mask_\\d+\\), assumeNotNull\\(rhs_mask_\\d+\\)\\)\\)\\) AS mask_\\d+\\).1\\) - toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(rhs_ip_\\d+\\), mask_\\d+\\).1\\)\\)\\)"
        },
        {
            "print ipv4_compare(A, B, C)",
            "SELECT if\\(\\(\\(multiIf\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS lhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS lhs_mask_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS rhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS rhs_mask_\\d+\\) IS NULL\\), NULL, sign\\(toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(lhs_ip_\\d+\\), toUInt8\\(min2\\(C, min2\\(assumeNotNull\\(lhs_mask_\\d+\\), assumeNotNull\\(rhs_mask_\\d+\\)\\)\\)\\) AS mask_\\d+\\).1\\) - toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(rhs_ip_\\d+\\), mask_\\d+\\).1\\)\\)\\)"
        },
        {
            "print ipv4_is_match(A, B)",
            "SELECT if\\(\\(\\(multiIf\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS lhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS lhs_mask_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS rhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS rhs_mask_\\d+\\) IS NULL\\), NULL, sign\\(toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(lhs_ip_\\d+\\), toUInt8\\(min2\\(32, min2\\(assumeNotNull\\(lhs_mask_\\d+\\), assumeNotNull\\(rhs_mask_\\d+\\)\\)\\)\\) AS mask_\\d+\\).1\\) - toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(rhs_ip_\\d+\\), mask_\\d+\\).1\\)\\)\\) = 0"
        },
        {
            "print ipv4_is_match(A, B, C)",
            "SELECT if\\(\\(\\(multiIf\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS lhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS lhs_mask_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS rhs_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS rhs_mask_\\d+\\) IS NULL\\), NULL, sign\\(toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(lhs_ip_\\d+\\), toUInt8\\(min2\\(C, min2\\(assumeNotNull\\(lhs_mask_\\d+\\), assumeNotNull\\(rhs_mask_\\d+\\)\\)\\)\\) AS mask_\\d+\\).1\\) - toInt64\\(IPv4CIDRToRange\\(assumeNotNull\\(rhs_ip_\\d+\\), mask_\\d+\\).1\\)\\)\\) = 0"
        },
        {
            "print parse_ipv4_mask(A, B)",
            "SELECT if\\(\\(\\(toIPv4OrNull\\(A\\) AS ip_\\d+\\) IS NULL\\) OR \\(\\(toUInt8OrNull\\(toString\\(B\\)\\) AS mask_\\d+\\) IS NULL\\), NULL, toUInt32\\(IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), toUInt8\\(max2\\(0, min2\\(32, assumeNotNull\\(mask_\\d+\\)\\)\\)\\)\\).1\\)\\)"
        },
        {
            "print ipv4_is_in_range(A, B)",
            "SELECT if\\(\\(\\(IPv4StringToNumOrNull\\(A\\) AS ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS range_start_ip_\\d+\\) IS NULL\\) OR \\(\\(multiIf\\(\\(length\\(splitByChar\\('/', B\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\) AS range_mask_\\d+\\) IS NULL\\), NULL, bitXor\\(range_start_ip_\\d+, bitAnd\\(ip_\\d+, bitNot\\(toUInt32\\(intExp2\\(32 - range_mask_\\d+\\) - 1\\)\\)\\)\\) = 0\\)"
        },
        {
            "print ipv4_is_private(A)",
            "SELECT multiIf\\(\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) > 2\\) OR \\(\\(toIPv4OrNull\\(tokens_\\d+\\[1\\]\\) AS nullable_ip_\\d+\\) IS NULL\\) OR \\(\\(length\\(tokens_\\d+\\) = 2\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL\\)\\), NULL, ignore\\(assumeNotNull\\(nullable_ip_\\d+\\) AS ip_\\d+, IPv4CIDRToRange\\(ip_\\d+, assumeNotNull\\(mask_\\d+\\)\\) AS range_\\d+, IPv4NumToString\\(range_\\d+.1\\) AS begin_\\d+, IPv4NumToString\\(range_\\d+.2\\) AS end_\\d+\\), NULL, \\(\\(length\\(tokens_\\d+\\) = 1\\) AND isIPAddressInRange\\(IPv4NumToString\\(ip_\\d+\\), '10.0.0.0/8'\\)\\) OR \\(\\(length\\(tokens_\\d+\\) = 2\\) AND isIPAddressInRange\\(begin_\\d+, '10.0.0.0/8'\\) AND isIPAddressInRange\\(end_\\d+, '10.0.0.0/8'\\)\\) OR \\(\\(length\\(tokens_\\d+\\) = 1\\) AND isIPAddressInRange\\(IPv4NumToString\\(ip_\\d+\\), '172.16.0.0/12'\\)\\) OR \\(\\(length\\(tokens_\\d+\\) = 2\\) AND isIPAddressInRange\\(begin_\\d+, '172.16.0.0/12'\\) AND isIPAddressInRange\\(end_\\d+, '172.16.0.0/12'\\)\\) OR \\(\\(length\\(tokens_\\d+\\) = 1\\) AND isIPAddressInRange\\(IPv4NumToString\\(ip_\\d+\\), '192.168.0.0/16'\\)\\) OR \\(\\(length\\(tokens_\\d+\\) = 2\\) AND isIPAddressInRange\\(begin_\\d+, '192.168.0.0/16'\\) AND isIPAddressInRange\\(end_\\d+, '192.168.0.0/16'\\)\\)\\)"
        },
        {
            "print ipv4_netmask_suffix(A)",
            "SELECT multiIf\\(\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) > 2\\) OR \\(NOT isIPv4String\\(tokens_\\d+\\[1\\]\\)\\), NULL, length\\(tokens_\\d+\\) = 1, 32, \\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NULL, NULL, toUInt8\\(min2\\(mask_\\d+, 32\\)\\)\\)"
        },
        {
            "print parse_ipv4(A)",
            "SELECT multiIf\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\)"
        },
        {
            "print parse_ipv6(A)",
            "SELECT if\\(\\(ifNull\\(if\\(\\(multiIf\\(length\\(splitByChar\\('/', A\\) AS tokens_\\d+\\) = 1, IPv4StringToNumOrNull\\(tokens_\\d+\\[1\\]\\) AS ip_\\d+, \\(length\\(tokens_\\d+\\) = 2\\) AND \\(ip_\\d+ IS NOT NULL\\) AND \\(\\(toUInt8OrNull\\(tokens_\\d+\\[-1\\]\\) AS mask_\\d+\\) IS NOT NULL\\), IPv4CIDRToRange\\(assumeNotNull\\(ip_\\d+\\), assumeNotNull\\(mask_\\d+\\)\\).1, NULL\\) AS ipv4_\\d+\\) IS NULL, NULL, IPv4ToIPv6\\(ipv4_\\d+\\)\\), IPv6StringToNumOrNull\\(A\\)\\) AS ipv6_\\d+\\) IS NULL, NULL, arrayStringConcat\\(flatten\\(extractAllGroups\\(lower\\(hex\\(assumeNotNull\\(ipv6_\\d+\\)\\)\\), '\\(\\[\\\\\\\\da-f\\]\\{4\\}\\)'\\)\\), ':'\\)\\)"
        }
})));
