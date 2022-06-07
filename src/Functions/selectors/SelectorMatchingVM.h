#pragma once

#include "Types.h"

#include <base/find_symbols.h>

#include <string>
#include <sstream>
#include <vector>

namespace DB
{

struct AttributeInfo
{
    AttributeMatcher matcher;
    bool matched = false;
};

inline const char * fillAttributes(std::vector<AttributeInfo> & attributes, const char * begin, const char * end)
{
    assert(*begin == '[');
    ++begin;
    while (true)
    {
        AttributeMatcher attribute;
        begin = find_first_not_symbols<' '>(begin, end);
        if (begin == end)
            return end;

        if (*begin == ']')
        {
            ++begin;
            if (begin == end || *begin != '[')
                return begin;

            ++begin;
        }

        const char * key_end = find_first_symbols<' ', '='>(begin, end);
        if (key_end == end)
            return end;

        attribute.key = std::string_view(begin, key_end - begin);

        begin = key_end;

        const char * eq_pos = find_first_symbols<'='>(begin, end);
        if (eq_pos + 1 < end)
            begin = eq_pos + 1;
        else
            return end;

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

        attribute.value_matcher = std::make_unique<ExactValueMatcher>(std::string_view(begin, value_end - begin));
        attributes.push_back({.matcher = std::move(attribute)});

        begin = value_end;

        if (*begin == '\'' || *begin == '"')
            ++begin;
    }
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

    std::string ToString() const
    {
        std::ostringstream ss;
        ss << "<" << expected_tag.value << ">";
        for (const auto & attr: attributes) {
            ss << "[" << attr.matched << ", " << attr.matcher.key.value << "=" << attr.matcher.value_matcher->pattern()
               << "]";
        }
        ss << "need_jmp=" << need_jump_to_next_instruction;
        return ss.str();
    }

    MatchResult match(CaseInsensitiveStringView tag_name) const
    {
        std::cout << "try match <" << tag_name.value << ">" << " [" << ToString() << "]\n";
        if (expected_tag.value == "*" || expected_tag == tag_name)
        {
            if (!attributes.empty()) {
                std::cout << "match result: need attributes\n";
                return MatchResult::NEED_ATTRIBUTES;
            }

            std::cout << "match result: match\n";
            return MatchResult::MATCH;
        }
        std::cout << "match result: not_match\n";
        return MatchResult::NOT_MATCH;
    }

    MatchResult handleAttribute(const Attribute & attribute)
    {
        std::cout << "handle attribute: " << attribute.key.value << '=' << attribute.value << " [" << ToString() << "]\n";
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
                    std::cout << "handleAttribute result: not_match\n";
                    return MatchResult::NOT_MATCH;
                }
            }
        }

        if (isAllAttributesMatched()) {
            reset();
            std::cout << "handleAttribute result: match\n";
            return MatchResult::MATCH;
        }
        std::cout << "handleAttribute result: need_attrs\n";
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

    void reset() {
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
    bool matched = false;
    CaseInsensitiveStringView last_matched_tag;
    int last_matched_tag_count = 0;

    void reset()
    {
        stack.clear();
        current_instruction_index = 0;
        for (auto & instruction : instructions)
            instruction.reset();
    }

    MatchResult handleOpeningTag(CaseInsensitiveStringView tag_name)
    {
        std::cout << "got opening tag " << tag_name.value << '\n';
        for (auto && entry: stack)
            std::cout << "<" << entry.tag_name.value << "> jump=" << entry.jump << ", next=" << entry.next << std::endl;
        if (matched) {
            if (tag_name == last_matched_tag)
                ++last_matched_tag_count;
            return MatchResult::MATCH;
        }

        last_tag_name = tag_name;
        last_stack_jump_index = stack.size() - 1;
        stack.push_back(StackItem{.tag_name = tag_name});

        auto match_result = instructions[current_instruction_index].match(tag_name);
        switch (match_result)
        {
            case MatchResult::MATCH:
                if (instructions[current_instruction_index].need_jump_to_next_instruction) {
                    stack.back().jump = current_instruction_index + 1;
                }
                ++current_instruction_index;
                if (current_instruction_index == instructions.size())
                {
                    current_instruction_index = 0;
                    matched = true;
                    last_matched_tag = tag_name;
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
                        std::cout << "found jump, executing, ind on stack = " << last_stack_jump_index <<  "\n";
                        auto mr = instructions[ind].match(tag_name);
                        switch (mr)
                        {
                            case MatchResult::MATCH:
                                current_instruction_index = ind + 1;
                                if (current_instruction_index == instructions.size())
                                {
                                    current_instruction_index = 0;
                                    matched = true;
                                    last_matched_tag = last_tag_name;
                                    last_matched_tag_count = 1;
                                    return MatchResult::MATCH;
                                }
                                stack.back().next = current_instruction_index;
                                if (instructions[ind].need_jump_to_next_instruction) {
                                    stack.back().jump = current_instruction_index;
                                }
                                return MatchResult::NOT_MATCH;
                            case MatchResult::NOT_MATCH:
                                continue;
                            case MatchResult::NEED_ATTRIBUTES:
                                current_instruction_index = ind;
                                --last_stack_jump_index;
                                return MatchResult::NEED_ATTRIBUTES;
                        }
                    }
                }

                current_instruction_index = 0;
                return MatchResult::NOT_MATCH;
            case MatchResult::NEED_ATTRIBUTES:
                return MatchResult::NEED_ATTRIBUTES;
        }
    }

    MatchResult handleClosingTag(CaseInsensitiveStringView tag_name)
    {
        std::cout << "got closing tag " << tag_name.value << '\n';
        for (auto && entry: stack)
            std::cout << "<" << entry.tag_name.value << "> jump=" << entry.jump << ", next=" << entry.next << std::endl;

        if (matched) {
            if (tag_name == last_matched_tag)
                --last_matched_tag_count;
            if (last_matched_tag_count == 0) {
                matched = false;
                stack.pop_back();
                return MatchResult::NOT_MATCH;
            } else {
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

    MatchResult handleAttribute(const Attribute & attribute) {
        auto match_result = instructions[current_instruction_index].handleAttribute(attribute);
        switch (match_result)
        {
            case MatchResult::MATCH:
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
                                return MatchResult::NOT_MATCH;
                            case MatchResult::NOT_MATCH:
                                continue;
                            case MatchResult::NEED_ATTRIBUTES:
                                current_instruction_index = ind;
                                --last_stack_jump_index;
                                return MatchResult::NEED_ATTRIBUTES;
                        }
                    }
                }

                current_instruction_index = 0;
                return MatchResult::NOT_MATCH;
            case MatchResult::NEED_ATTRIBUTES:
                return MatchResult::NEED_ATTRIBUTES;
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

            const char * next = find_first_symbols<' ', '['>(begin, end);

            Instruction instruction;

            instruction.expected_tag = std::string_view(begin, next - begin);

            if (next != end && *next == '[')
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
