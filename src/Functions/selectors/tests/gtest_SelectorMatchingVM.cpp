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

TEST(SelectorMatchingVM, AttributeSelectors)
{
    std::string selector = "body[key0][key1=value1][key2 *= 'value2'][key3 ^= \"value3\"][key4 $='value4'][key5 ~= value5]";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 1);
    EXPECT_EQ(vm.instructions[0].expected_tag, "body"sv);
    EXPECT_FALSE(vm.instructions[0].need_jump_to_next_instruction);

    const auto& attributes = vm.instructions[0].attributes;
    ASSERT_EQ(attributes.size(), 6);

    EXPECT_EQ(attributes[0].matcher.key, "key0"sv);
    EXPECT_EQ(attributes[0].matcher.value_matcher->getPattern(), "");
    EXPECT_TRUE(attributes[0].matcher.value_matcher->match("anything"));

    EXPECT_EQ(attributes[1].matcher.key, "key1"sv);
    EXPECT_EQ(attributes[1].matcher.value_matcher->getPattern(), "value1");
    EXPECT_TRUE(attributes[1].matcher.value_matcher->match("value1"));
    EXPECT_FALSE(attributes[1].matcher.value_matcher->match("not_value1"));

    EXPECT_EQ(attributes[2].matcher.key, "key2"sv);
    EXPECT_EQ(attributes[2].matcher.value_matcher->getPattern(), "value2");
    EXPECT_TRUE(attributes[2].matcher.value_matcher->match("sometextsvalue2sometext"));
    EXPECT_FALSE(attributes[2].matcher.value_matcher->match("sometext"));

    EXPECT_EQ(attributes[3].matcher.key, "key3"sv);
    EXPECT_EQ(attributes[3].matcher.value_matcher->getPattern(), "value3");
    EXPECT_TRUE(attributes[3].matcher.value_matcher->match("value3sometext"));
    EXPECT_FALSE(attributes[3].matcher.value_matcher->match("sometext"));

    EXPECT_EQ(attributes[4].matcher.key, "key4"sv);
    EXPECT_EQ(attributes[4].matcher.value_matcher->getPattern(), "value4");
    EXPECT_TRUE(attributes[4].matcher.value_matcher->match("sometextvalue4"));
    EXPECT_FALSE(attributes[4].matcher.value_matcher->match("sometext"));

    EXPECT_EQ(attributes[5].matcher.key, "key5"sv);
    EXPECT_EQ(attributes[5].matcher.value_matcher->getPattern(), "value5");
    EXPECT_TRUE(attributes[5].matcher.value_matcher->match("some text value5 some text"));
    EXPECT_FALSE(attributes[5].matcher.value_matcher->match("some text value5some text"));
}

TEST(SelectorMatchingVM, AttributeSelectorsClassID)
{
    std::string selector = "body.value0[key1=value1]#value2 a.value3 > span#value4";
    const char * begin = selector.data();
    const char * end = selector.data() + selector.size();
    auto vm = SelectorMatchingVM::parseSelector(begin, end);

    ASSERT_EQ(vm.instructions.size(), 3);
    EXPECT_EQ(vm.instructions[0].expected_tag, "body"sv);
    EXPECT_TRUE(vm.instructions[0].need_jump_to_next_instruction);
    EXPECT_EQ(vm.instructions[1].expected_tag, "a"sv);
    EXPECT_FALSE(vm.instructions[1].need_jump_to_next_instruction);
    EXPECT_EQ(vm.instructions[2].expected_tag, "span"sv);
    EXPECT_FALSE(vm.instructions[2].need_jump_to_next_instruction);

    {
        const auto & attributes = vm.instructions[0].attributes;
        ASSERT_EQ(attributes.size(), 3);

        EXPECT_EQ(attributes[0].matcher.key, "class"sv);
        EXPECT_EQ(attributes[0].matcher.value_matcher->getPattern(), "value0");
        EXPECT_TRUE(attributes[0].matcher.value_matcher->match("value0 value1"));
        EXPECT_FALSE(attributes[0].matcher.value_matcher->match("value2 value1"));

        EXPECT_EQ(attributes[1].matcher.key, "key1"sv);
        EXPECT_EQ(attributes[1].matcher.value_matcher->getPattern(), "value1");
        EXPECT_TRUE(attributes[1].matcher.value_matcher->match("value1"));
        EXPECT_FALSE(attributes[1].matcher.value_matcher->match("not_value1"));

        EXPECT_EQ(attributes[2].matcher.key, "id"sv);
        EXPECT_EQ(attributes[2].matcher.value_matcher->getPattern(), "value2");
        EXPECT_TRUE(attributes[2].matcher.value_matcher->match("value2"));
        EXPECT_FALSE(attributes[2].matcher.value_matcher->match("value1"));
    }
    {
        const auto & attributes = vm.instructions[1].attributes;
        ASSERT_EQ(attributes.size(), 1);
        EXPECT_EQ(attributes[0].matcher.key, "class"sv);
        EXPECT_EQ(attributes[0].matcher.value_matcher->getPattern(), "value3");
        EXPECT_TRUE(attributes[0].matcher.value_matcher->match("value3 value1"));
        EXPECT_FALSE(attributes[0].matcher.value_matcher->match("value2 value1"));
    }

    {
        const auto & attributes = vm.instructions[2].attributes;
        ASSERT_EQ(attributes.size(), 1);
        EXPECT_EQ(attributes[0].matcher.key, "id"sv);
        EXPECT_EQ(attributes[0].matcher.value_matcher->getPattern(), "value4");
        EXPECT_TRUE(attributes[0].matcher.value_matcher->match("value4"));
        EXPECT_FALSE(attributes[0].matcher.value_matcher->match("not value4"));
    }
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
    EXPECT_EQ(attrs[0].matcher.value_matcher->getPattern(), "value1");
    EXPECT_FALSE(attrs[0].matched);

    EXPECT_EQ(attrs[1].matcher.key, "key2"sv);
    EXPECT_EQ(attrs[1].matcher.value_matcher->getPattern(), "value2");
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
    EXPECT_EQ(attrs[0].matcher.value_matcher->getPattern(), "value1");
    EXPECT_FALSE(attrs[0].matched);

    EXPECT_EQ(attrs[1].matcher.key, "key2"sv);
    EXPECT_EQ(attrs[1].matcher.value_matcher->getPattern(), "value2");
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
