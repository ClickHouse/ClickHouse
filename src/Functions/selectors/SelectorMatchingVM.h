#pragma once

#include "Types.h"
#include "ValueMatchers.h"

#include <base/find_symbols.h>

#include <sstream>
#include <string>
#include <vector>

namespace DB
{

struct AttributeInfo
{
    AttributeSelector matcher;
    bool matched = false;
};

inline const char * fillAttributes(std::vector<AttributeInfo> & attributes, const char * begin, const char * end)
{
    while (begin != end && *begin != ' ')
    {
        AttributeSelector attribute;

        if (*begin == ']')
        {
            ++begin;
            if (begin == end || (*begin != '[' && *begin != '.' && *begin != '#'))
                return begin;
        }

        const char attr_spec = *begin;
        begin = find_first_not_symbols<' '>(begin + 1, end);

        const char * word_end = find_first_symbols<' ', ']', '[', '=', '*', '^', '$', '~'>(begin, end);
        auto word = std::string_view(begin, word_end - begin);
        begin = word_end;

        if (attr_spec == '.')
        {
            attribute.value_matcher = MakeContainsWordMatcher(word);
            attribute.key = CaseInsensitiveStringView{"class"};
            attributes.push_back({.matcher = std::move(attribute)});
            continue;
        }
        else if (attr_spec == '#')
        {
            attribute.value_matcher = MakeExactMatcher(word);
            attribute.key = CaseInsensitiveStringView{"id"};
            attributes.push_back({.matcher = std::move(attribute)});
            continue;
        }
        else if (attr_spec == '[')
        {
            attribute.key = CaseInsensitiveStringView{word};
        }

        const char * eq_pos = find_first_symbols<']', '=', '*', '^', '$', '~'>(begin, end);

        if (eq_pos == end) {
            // TODO: throw an exception
            return end;
        }

        if (*eq_pos == ']')
        {
            attribute.value_matcher = MakeAlwaysTrueMatcher();
            attributes.push_back({.matcher = std::move(attribute)});
            begin = eq_pos;
            continue;
        }

        if (eq_pos + 1 < end)
            begin = eq_pos + 1;
        else
            return end;

        if (*begin == '=')
            ++begin;

        begin = find_first_not_symbols<' '>(begin, end);
        if (begin == end || *begin == ']')
            return end;

        const char * value_end = end;
        if (*begin == '\'')
        {
            ++begin;
            value_end = find_first_symbols<'\''>(begin, value_end);
        }
        else if (*begin == '"')
        {
            ++begin;
            value_end = find_first_symbols<'"'>(begin, value_end);
        }
        else
        {
            value_end = find_first_symbols<' ', ']'>(begin, value_end);
        }

        if (value_end == end)
            return end;

        switch (*eq_pos)
        {
            case '=':
                attribute.value_matcher = MakeExactMatcher(std::string_view(begin, value_end - begin));
                break;
            case '*':
                attribute.value_matcher = MakeSubstringMatcher(std::string_view(begin, value_end - begin));
                break;
            case '^':
                attribute.value_matcher = MakeStartsWithMatcher(std::string_view(begin, value_end - begin));
                break;
            case '$':
                attribute.value_matcher = MakeEndsWithMatcher(std::string_view(begin, value_end - begin));
                break;
            case '~':
                attribute.value_matcher = MakeContainsWordMatcher(std::string_view(begin, value_end - begin));
                break;
            default:
                // unreachable
                return end;
        }
        attributes.push_back({.matcher = std::move(attribute)});

        begin = value_end;

        if (*begin == '\'' || *begin == '"')
            ++begin;
    }

    return begin;
}

enum class MatchResult
{
    MATCH = 0,
    NOT_MATCH = 1,
    NEED_ATTRIBUTES = 2,
};

struct StackItem
{
    CaseInsensitiveStringView tag_name;
    size_t next = 0;
    int64_t jump = -1;
};

struct Instruction
{
    CaseInsensitiveStringView expected_tag;
    std::vector<AttributeInfo> attributes;
    bool need_jump_to_next_instruction = false;

    MatchResult match(CaseInsensitiveStringView tag_name) const
    {
        if (expected_tag.value == "*" || expected_tag == tag_name)
        {
            if (!attributes.empty())
                return MatchResult::NEED_ATTRIBUTES;

            return MatchResult::MATCH;
        }
        return MatchResult::NOT_MATCH;
    }

    MatchResult handleAttribute(const Attribute & attribute)
    {
        for (auto & attr : attributes)
        {
            if (!attr.matched && attr.matcher.key == attribute.key)
            {
                if (attr.matcher.match(attribute.value))
                {
                    attr.matched = true;
                }
                else
                {
                    reset();
                    return MatchResult::NOT_MATCH;
                }
            }
        }

        if (isAllAttributesMatched())
        {
            reset();
            return MatchResult::MATCH;
        }
        return MatchResult::NEED_ATTRIBUTES;
    }

    bool isAllAttributesMatched() const
    {
        for (const auto & attr : attributes)
        {
            if (!attr.matched)
                return false;
        }
        return true;
    }

    void reset()
    {
        for (auto & attr : attributes)
            attr.matched = false;
    }
};


struct SelectorMatchingVM
{
    std::vector<Instruction> instructions;
    std::vector<StackItem> stack;
    size_t current_instruction_index = 0;

    int last_stack_jump_index = -1;
    CaseInsensitiveStringView last_tag_name;
    std::vector<Attribute> last_tag_attributes;
    size_t cur_attribute_ind = 0;

    bool matched = false;
    CaseInsensitiveStringView last_matched_tag;
    int last_matched_tag_count = 0;

    void reset()
    {
        stack.clear();
        current_instruction_index = 0;

        last_tag_name = {};
        last_tag_attributes.clear();
        cur_attribute_ind = 0;
        last_stack_jump_index = -1;

        matched = false;
        last_matched_tag = {};
        last_matched_tag_count = 0;

        for (auto & instruction : instructions)
            instruction.reset();
    }

    MatchResult handleOpeningTag(CaseInsensitiveStringView tag_name)
    {
        if (matched)
        {
            if (tag_name == last_matched_tag)
                ++last_matched_tag_count;
            return MatchResult::MATCH;
        }

        last_tag_name = tag_name;
        last_tag_attributes.clear();
        cur_attribute_ind = 0;

        last_stack_jump_index = stack.size() - 1;
        stack.push_back(StackItem{.tag_name = tag_name});

        return dispatchInstructionResult(instructions[current_instruction_index].match(tag_name));
    }

    MatchResult handleClosingTag(CaseInsensitiveStringView tag_name)
    {
        if (matched)
        {
            if (tag_name == last_matched_tag)
                --last_matched_tag_count;

            if (last_matched_tag_count == 0)
            {
                matched = false;
                stack.pop_back();
                return MatchResult::NOT_MATCH;
            }
            else
            {
                return MatchResult::MATCH;
            }
        }

        // remove self-closing tags
        while (!stack.empty() && tag_name != stack.back().tag_name)
            stack.pop_back();

        // TODO: ??? assert(!stack.empty() && tag_name == stack.back().tag_name);
        if (!stack.empty())
            stack.pop_back();

        if (!stack.empty())
            current_instruction_index = stack.back().next;
        else
            current_instruction_index = 0;

        return MatchResult::NOT_MATCH;
    }

    MatchResult handleAttribute(const Attribute & attribute)
    {
        last_tag_attributes.push_back(attribute);
        return dispatchInstructionResult(instructions[current_instruction_index].handleAttribute(attribute));
    }

    MatchResult dispatchInstructionResult(MatchResult match_result)
    {
        switch (match_result)
        {
            case MatchResult::MATCH:
                if (instructions[current_instruction_index].need_jump_to_next_instruction)
                {
                    stack.back().jump = current_instruction_index + 1;
                }
                ++current_instruction_index;
                if (current_instruction_index == instructions.size())
                {
                    current_instruction_index = 0;
                    matched = true;
                    last_matched_tag = last_tag_name;
                    last_matched_tag_count = 1;
                    return MatchResult::MATCH;
                }
                stack.back().next = current_instruction_index;
                return MatchResult::NOT_MATCH;
            case MatchResult::NOT_MATCH:
                // TODO: store index of last item with jump in every stack item
                for (; last_stack_jump_index >= 0; --last_stack_jump_index)
                {
                    auto ind = stack[last_stack_jump_index].jump;
                    if (ind != -1)
                    {
                        auto mr = instructions[ind].match(last_tag_name);
                        switch (mr)
                        {
                            case MatchResult::MATCH:
                                current_instruction_index = ind + 1;
                                if (current_instruction_index == instructions.size())
                                {
                                    current_instruction_index = 0;
                                    return MatchResult::MATCH;
                                }
                                stack.back().next = current_instruction_index;
                                if (instructions[ind].need_jump_to_next_instruction)
                                {
                                    stack.back().jump = current_instruction_index;
                                }
                                return MatchResult::NOT_MATCH;
                            case MatchResult::NOT_MATCH:
                                cur_attribute_ind = 0;
                                continue;
                            case MatchResult::NEED_ATTRIBUTES:
                                current_instruction_index = ind;
                                --last_stack_jump_index;
                                if (cur_attribute_ind == last_tag_attributes.size())
                                {
                                    return MatchResult::NEED_ATTRIBUTES;
                                }
                                return dispatchInstructionResult(
                                    instructions[current_instruction_index].handleAttribute(last_tag_attributes[cur_attribute_ind++]));
                        }
                    }
                }
                cur_attribute_ind = 0;
                if (current_instruction_index != 0)
                {
                    current_instruction_index = 0;
                    return dispatchInstructionResult(instructions[current_instruction_index].match(last_tag_name));
                }

                current_instruction_index = 0;
                return MatchResult::NOT_MATCH;
            case MatchResult::NEED_ATTRIBUTES:
                if (cur_attribute_ind >= last_tag_attributes.size())
                {
                    return MatchResult::NEED_ATTRIBUTES;
                }
                return dispatchInstructionResult(
                    instructions[current_instruction_index].handleAttribute(last_tag_attributes[cur_attribute_ind++]));
        }
    }

    static SelectorMatchingVM parseSelector(const char * begin, const char * end)
    {
        SelectorMatchingVM vm;
        while (begin != end)
        {
            bool need_jump = !vm.instructions.empty();
            begin = find_first_not_symbols<' '>(begin, end);

            if (begin == end)
                break;

            if (*begin == '>')
            {
                need_jump = false;

                ++begin;
                begin = find_first_not_symbols<' '>(begin, end);
                if (begin == end)
                    break;
            }

            const char * next = find_first_symbols<' ', '[', '#', '.'>(begin, end);

            Instruction instruction;

            instruction.expected_tag = std::string_view(begin, next - begin);
            if (instruction.expected_tag == std::string_view{})
                instruction.expected_tag = std::string_view{"*"};

            if (next != end && *next != ' ')
                begin = fillAttributes(instruction.attributes, next, end);
            else
                begin = next;

            if (need_jump)
                vm.instructions.back().need_jump_to_next_instruction = true;

            vm.instructions.push_back(std::move(instruction));
        }
        return vm;
    }
};

}
