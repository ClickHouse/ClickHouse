#include <Functions/selectors/SelectorMatchingVM.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace std::string_view_literals;

TEST(SelectorMatchingVM, SingleTag)
{
    std::string selector = "body";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 1);
    EXPECT_EQ(vm.instructions[0].expected_tag, "body"sv);
    EXPECT_TRUE(vm.instructions[0].attributes.empty());
    EXPECT_FALSE(vm.instructions[0].need_jump_to_next_instruction);

    EXPECT_EQ(vm.handleOpeningTag("a"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.handleOpeningTag("bOdY"sv), MatchResult::MATCH);
}

TEST(SelectorMatchingVM, SingleTagWithAttributes)
{
    std::string selector = "body[key1='value1'][key2 = \"value2\"]";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 1);
    EXPECT_EQ(vm.instructions[0].expected_tag, "body"sv);

    const auto & attrs = vm.instructions[0].attributes;
    ASSERT_EQ(attrs.size(), 2);

    EXPECT_EQ(attrs[0].matcher.key, "key1"sv);
    EXPECT_EQ(attrs[0].matcher.value_matcher->pattern(), "value1");
    EXPECT_FALSE(attrs[0].matched);

    EXPECT_EQ(attrs[1].matcher.key, "key2"sv);
    EXPECT_EQ(attrs[1].matcher.value_matcher->pattern(), "value2");
    EXPECT_FALSE(attrs[1].matched);

    EXPECT_FALSE(vm.instructions[0].need_jump_to_next_instruction);

    EXPECT_EQ(vm.handleOpeningTag("a"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.handleOpeningTag("body"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"unknown_key"sv, "value2"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value2"}), MatchResult::MATCH);

    vm.reset();

    EXPECT_EQ(vm.handleOpeningTag("body"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "unknown_value"}), MatchResult::NOT_MATCH);
}

TEST(SelectorMatchingVM, Combinators)
{
    std::string selector = "body > div span";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 3);
    EXPECT_EQ(vm.instructions[0].expected_tag, "body"sv);
    EXPECT_TRUE(vm.instructions[0].attributes.empty());
    EXPECT_FALSE(vm.instructions[0].need_jump_to_next_instruction);
    EXPECT_EQ(vm.instructions[1].expected_tag, "div"sv);
    EXPECT_TRUE(vm.instructions[1].attributes.empty());
    EXPECT_TRUE(vm.instructions[1].need_jump_to_next_instruction);
    EXPECT_EQ(vm.instructions[2].expected_tag, "span"sv);
    EXPECT_TRUE(vm.instructions[2].attributes.empty());
    EXPECT_FALSE(vm.instructions[2].need_jump_to_next_instruction);

    EXPECT_EQ(vm.handleOpeningTag("body"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 1);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 2);

    EXPECT_EQ(vm.handleOpeningTag("a"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleOpeningTag("span"sv), MatchResult::MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleOpeningTag("span"sv), MatchResult::MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    vm.handleClosingTag("span"sv);
    vm.handleClosingTag("span"sv);
    vm.handleClosingTag("div"sv);
    vm.handleClosingTag("a"sv);
    vm.handleClosingTag("div"sv);
    EXPECT_EQ(vm.current_instruction_index, 1);

    vm.handleClosingTag("body"sv);
    EXPECT_EQ(vm.current_instruction_index, 0);
}

void expectKeyValue(const Instruction& instruction) {
    const auto & attrs = instruction.attributes;
    ASSERT_EQ(attrs.size(), 2);

    EXPECT_EQ(attrs[0].matcher.key, "key1"sv);
    EXPECT_EQ(attrs[0].matcher.value_matcher->pattern(), "value1");
    EXPECT_FALSE(attrs[0].matched);

    EXPECT_EQ(attrs[1].matcher.key, "key2"sv);
    EXPECT_EQ(attrs[1].matcher.value_matcher->pattern(), "value2");
    EXPECT_FALSE(attrs[1].matched);
}

TEST(SelectorMatchingVM, CombinatorsWithAttributes)
{
    std::string selector
        = R"(body[key1='value1'][key2 = "value2"] > div[key1=value1][key2 = value2] span[key1='value1'][key2 = "value2"])";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 3);

    EXPECT_EQ(vm.instructions[0].expected_tag, "body"sv);
    EXPECT_FALSE(vm.instructions[0].need_jump_to_next_instruction);

    EXPECT_EQ(vm.instructions[1].expected_tag, "div"sv);
    EXPECT_TRUE(vm.instructions[1].need_jump_to_next_instruction);

    EXPECT_EQ(vm.instructions[2].expected_tag, "span"sv);
    EXPECT_FALSE(vm.instructions[2].need_jump_to_next_instruction);

    expectKeyValue(vm.instructions[0]);
    expectKeyValue(vm.instructions[1]);
    expectKeyValue(vm.instructions[2]);

    EXPECT_EQ(vm.handleOpeningTag("body"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"unknown_key"sv, "value2"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value2"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 1);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"unknown_key"sv, "value2"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value2"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 2);

    EXPECT_EQ(vm.handleOpeningTag("a"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleOpeningTag("span"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"unknown_key"sv, "value2"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value2"}), MatchResult::MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleOpeningTag("any_tag"sv), MatchResult::MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);

    EXPECT_EQ(vm.handleClosingTag("any_tag"sv), MatchResult::MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);
    EXPECT_EQ(vm.handleClosingTag("span"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);
    EXPECT_EQ(vm.handleClosingTag("div"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);
    EXPECT_EQ(vm.handleClosingTag("a"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 2);

    EXPECT_EQ(vm.handleClosingTag("div"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 1);

    EXPECT_EQ(vm.handleClosingTag("body"sv), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 0);
}

TEST(SelectorMatchingVM, DescendantWithAttributes)
{
    std::string selector
        = R"(div[key1='value1'][key2 = "value2"] div[key1=value3][key2 = value4] div[key1='value5'][key2 = "value6"] a)";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 4);

    EXPECT_EQ(vm.instructions[0].expected_tag, "div"sv);
    EXPECT_TRUE(vm.instructions[0].need_jump_to_next_instruction);

    EXPECT_EQ(vm.instructions[1].expected_tag, "div"sv);
    EXPECT_TRUE(vm.instructions[1].need_jump_to_next_instruction);

    EXPECT_EQ(vm.instructions[2].expected_tag, "div"sv);
    EXPECT_TRUE(vm.instructions[2].need_jump_to_next_instruction);

    EXPECT_EQ(vm.instructions[3].expected_tag, "a"sv);
    EXPECT_FALSE(vm.instructions[3].need_jump_to_next_instruction);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value2"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 1);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value3"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value4"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 2);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value5"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value6"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 3);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value1"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value2"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 1);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value5"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value6"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 3);

    EXPECT_EQ(vm.handleOpeningTag("div"sv), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key1"sv, "value3"}), MatchResult::NEED_ATTRIBUTES);
    EXPECT_EQ(vm.handleAttribute(Attribute{"key2"sv, "value4"}), MatchResult::NOT_MATCH);
    EXPECT_EQ(vm.current_instruction_index, 2);

    EXPECT_EQ(vm.handleOpeningTag("a"sv), MatchResult::MATCH);
}
