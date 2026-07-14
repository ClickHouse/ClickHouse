#pragma once

#include <Parsers/IParser.h>

#include <gtest/gtest.h>

#include <string_view>

struct ParserTestCase
{
    const std::string_view input_text;
    const char * expected_ast = nullptr;
};

class ParserTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
{};
class ParserKQLTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
{};
class ParserRegexTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<DB::IParser>, ParserTestCase>>
{};
