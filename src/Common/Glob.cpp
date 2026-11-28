#include <Common/Glob.h>

#include <Common/Exception.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool isDigit(char c)
{
    return c >= '0' && c <= '9';
}

/// Appends a decimal digit to `value`. Returns false if the result does not fit into 64 bits.
bool appendDigit(UInt64 & value, char digit)
{
    const UInt64 addend = static_cast<UInt64>(digit - '0');
    if (value > (std::numeric_limits<UInt64>::max() - addend) / 10)
        return false;
    value = value * 10 + addend;
    return true;
}

/// The length of the character that starts at `pos`, or zero if the bytes there do not form one.
/// This is what the character classes `re2` builds for `[^/]` and `[^{}]` take for a character
/// above 0x7F: it permits overlong encodings in three and four byte sequences and code points above
/// 0x10FFFF in the latter.
size_t characterLength(std::string_view s, size_t pos)
{
    const auto lead = static_cast<unsigned char>(s[pos]);
    if (lead < 0x80)
        return 1;

    size_t length = 0;
    if (lead >= 0xC2 && lead <= 0xDF)
        length = 2;
    else if (lead >= 0xE0 && lead <= 0xEF)
        length = 3;
    else if (lead >= 0xF0 && lead <= 0xF4)
        length = 4;
    else
        return 0;

    if (s.size() - pos < length)
        return 0;

    for (size_t i = 1; i < length; ++i)
        if ((static_cast<unsigned char>(s[pos + i]) & 0xC0) != 0x80)
            return 0;

    return length;
}

/// The characters `makeRegexpPatternFromGlobs` escapes before it looks for groups. In front of the
/// search for an enum each of them counts as two.
bool isEscapedBeforeGroups(char c)
{
    return c == '[' || c == ']' || c == '|' || c == '+' || c == '-' || c == '(' || c == ')' || c == '\\';
}

/// The characters that do not match themselves in the text between groups and in an alternative.
constexpr std::string_view SPECIAL_IN_TEXT = "*?^$";

bool isSpecialInText(char c)
{
    return SPECIAL_IN_TEXT.contains(c);
}

}


Glob::Glob(std::string pattern_)
    : pattern(std::move(pattern_))
{
    compile();
}

void Glob::addOp(OpType type, size_t first, size_t second)
{
    if (program.size() >= MAX_PROGRAM_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The path with globs is too complex: it has more than {} wildcards and alternatives.",
                        MAX_PROGRAM_SIZE);

    program.push_back(Op{.type = type, .first = first, .second = second});
}

void Glob::compile()
{
    /// Where the text that follows the last group begins.
    size_t text_begin = 0;
    size_t pos = 0;

    while (true)
    {
        const size_t open = pattern.find('{', pos);
        if (open == std::string::npos)
            break;

        const size_t next_brace = pattern.find_first_of("{}", open + 1);
        if (next_brace == std::string::npos)
            break;

        /// A body does not hold a brace, so this '{' does not open a group, but the next one may.
        if (pattern[next_brace] == '{')
        {
            pos = next_brace;
            continue;
        }

        const size_t body_begin = open + 1;
        const size_t body_end = next_brace;

        Range range;
        if (tryParseRange(body_begin, body_end, range))
        {
            compileText(text_begin, open);
            addOp(OpType::Range, ranges.size());
            ranges.push_back(range);
            text_begin = next_brace + 1;
        }
        else if (isEnum(body_begin, body_end))
        {
            compileText(text_begin, open);
            compileEnum(body_begin, body_end);
            text_begin = next_brace + 1;
        }

        /// Otherwise both braces stay in the text as literal characters.
        pos = next_brace + 1;
    }

    compileText(text_begin, pattern.size());
    addOp(OpType::Match);
}

bool Glob::tryParseRange(size_t begin, size_t end, Range & range) const
{
    size_t first_end = begin;
    while (first_end < end && isDigit(pattern[first_end]))
        ++first_end;

    if (first_end == begin || end - first_end < 3 || pattern[first_end] != '.' || pattern[first_end + 1] != '.')
        return false;

    const size_t second_begin = first_end + 2;
    for (size_t i = second_begin; i < end; ++i)
        if (!isDigit(pattern[i]))
            return false;

    UInt64 first = 0;
    UInt64 second = 0;
    bool fits = true;
    for (size_t i = begin; i < first_end; ++i)
        fits = fits && appendDigit(first, pattern[i]);
    for (size_t i = second_begin; i < end; ++i)
        fits = fits && appendDigit(second, pattern[i]);

    if (!fits)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The range glob '{{{}}}' in the path has a bound that does not fit into 64 bits.",
                        std::string_view(pattern).substr(begin, end - begin));

    /// The smaller bound decides how the numbers are written; it is the first one when they are equal.
    const bool descending = first > second;
    const size_t min_begin = descending ? second_begin : begin;
    const size_t min_width = descending ? end - second_begin : first_end - begin;
    const size_t max_width = descending ? first_end - begin : end - second_begin;

    range.min = descending ? second : first;
    range.max = descending ? first : second;

    /// A single '0' does not ask for padding: `{0..10}`.
    const bool padded = pattern[min_begin] == '0' && min_width > 1;
    range.width = padded ? std::max(min_width, max_width) : 0;

    return true;
}

bool Glob::isEnum(size_t begin, size_t end) const
{
    if (begin == end || pattern[begin] == ',' || pattern[end - 1] == ',')
        return false;

    /// The body has to be two characters long for the regexp that looks for an enum, and it looks at
    /// the pattern after some of its characters were escaped with a backslash.
    size_t num_characters = 0;
    for (size_t i = begin; i < end && num_characters < 2; ++i)
    {
        if ((static_cast<unsigned char>(pattern[i]) & 0xC0) != 0x80)
            ++num_characters;
        if (isEscapedBeforeGroups(pattern[i]))
            ++num_characters;
    }

    return num_characters >= 2;
}

void Glob::compileEnum(size_t begin, size_t end)
{
    VectorWithMemoryTracking<LiteralAlternative> literals;
    /// The alternatives that hold a wildcard, as ranges of offsets in the pattern.
    VectorWithMemoryTracking<std::pair<size_t, size_t>> others;

    const std::string_view body = std::string_view(pattern).substr(0, end);

    size_t alternative_begin = begin;
    while (true)
    {
        size_t alternative_end = body.find(',', alternative_begin);
        if (alternative_end == std::string_view::npos)
            alternative_end = end;

        const bool is_literal
            = body.substr(alternative_begin, alternative_end - alternative_begin).find_first_of(SPECIAL_IN_TEXT) == std::string_view::npos;

        if (is_literal)
            literals.push_back(LiteralAlternative{.offset = alternative_begin, .size = alternative_end - alternative_begin});
        else
            others.emplace_back(alternative_begin, alternative_end);

        if (alternative_end == end)
            break;
        alternative_begin = alternative_end + 1;
    }

    if (others.empty())
    {
        compileLiteralAlternatives(literals);
        return;
    }

    if (literals.empty() && others.size() == 1)
    {
        compileText(others.front().first, others.front().second);
        return;
    }

    /// All the literal alternatives make one branch, and every other alternative makes one of its own.
    const size_t num_branches = others.size() + (literals.empty() ? 0 : 1);
    const size_t first_target = branch_targets.size();
    branch_targets.resize(first_target + num_branches);
    addOp(OpType::Alt, first_target, num_branches);

    /// The jumps that end the branches. Where the enum ends is known only after the last of them.
    VectorWithMemoryTracking<size_t> jumps;
    size_t branch = 0;

    if (!literals.empty())
    {
        branch_targets[first_target + branch++] = program.size();
        compileLiteralAlternatives(literals);
        jumps.push_back(program.size());
        addOp(OpType::Jump);
    }

    for (const auto & [other_begin, other_end] : others)
    {
        branch_targets[first_target + branch++] = program.size();
        compileText(other_begin, other_end);
        jumps.push_back(program.size());
        addOp(OpType::Jump);
    }

    for (const size_t jump : jumps)
        program[jump].first = program.size();
}

void Glob::compileLiteralAlternatives(VectorWithMemoryTracking<LiteralAlternative> & alternatives)
{
    const std::string_view text(pattern);

    auto bytes = [&](const LiteralAlternative & alternative) { return text.substr(alternative.offset, alternative.size); };

    std::sort(alternatives.begin(), alternatives.end(), [&](const LiteralAlternative & lhs, const LiteralAlternative & rhs)
    {
        if (lhs.size != rhs.size)
            return lhs.size < rhs.size;
        return bytes(lhs) < bytes(rhs);
    });

    const size_t first_group = literal_groups.size();

    for (const auto & alternative : alternatives)
    {
        const bool starts_group = literal_groups.size() == first_group || literal_groups.back().size != alternative.size;

        /// The same alternative written twice.
        if (!starts_group && bytes(literal_alternatives.back()) == bytes(alternative))
            continue;

        if (starts_group)
            literal_groups.push_back(LiteralGroup{.size = alternative.size, .first = literal_alternatives.size(), .count = 0});

        literal_alternatives.push_back(alternative);
        ++literal_groups.back().count;
    }

    addOp(OpType::AltLiteral, first_group, literal_groups.size() - first_group);
}

void Glob::compileText(size_t begin, size_t end)
{
    size_t literal_begin = begin;

    auto flush_literal = [&](size_t literal_end)
    {
        if (literal_end > literal_begin)
            addOp(OpType::Literal, literal_begin, literal_end - literal_begin);
    };

    /// Whether the last thing before this one, a `?` aside, was a `*`.
    bool after_star = false;

    size_t i = begin;
    while (i < end)
    {
        const char c = pattern[i];

        if (!isSpecialInText(c))
        {
            after_star = false;
            ++i;
            continue;
        }

        flush_literal(i);

        if (c == '?')
        {
            /// Does not change `after_star`: `*?*` is a `*` followed by a `*`.
            addOp(OpType::AnyOne);
            ++i;
        }
        else if (c == '^')
        {
            addOp(OpType::AssertBegin);
            after_star = false;
            ++i;
        }
        else if (c == '$')
        {
            addOp(OpType::AssertEnd);
            after_star = false;
            ++i;
        }
        else if (i + 2 < end && pattern[i + 1] == '*' && pattern[i + 2] == '/'
            && (i == 0 || (i > begin && pattern[i - 1] == '/')))
        {
            /// `**/` as a whole path component. The component has to begin in this very text: at the
            /// beginning of an alternative and right after a group the `**` is two stars.
            addOp(OpType::Globstar);
            after_star = false;
            i += 3;
        }
        else
        {
            addOp(after_star ? OpType::StarNoBrace : OpType::Star);
            after_star = true;
            ++i;
        }

        literal_begin = i;
    }

    flush_literal(end);
}

bool Glob::matches(std::string_view candidate) const
{
    /// The pattern is a nondeterministic automaton, and a pair of an instruction and a position in the
    /// candidate is its state. The search below visits every reachable state at most once, so it takes
    /// time proportional to their number at most, where trying every way to match takes exponential
    /// time on a pattern like `*a*a*a*b`.
    /// The text the pattern ends with, if it does, rules most of the candidates out at once.
    if (program.size() >= 2)
    {
        const Op & last = program[program.size() - 2];
        if (last.type == OpType::Literal && !candidate.ends_with(std::string_view(pattern).substr(last.first, last.second)))
            return false;
    }

    const size_t num_positions = candidate.size() + 1;
    const size_t num_states = program.size() * num_positions;
    const size_t num_words = (num_states + 63) / 64;

    /// Most patterns and candidates are small, and matching them does not allocate.
    static constexpr size_t INLINE_VISITED_WORDS = 64;
    static constexpr size_t INLINE_STACK_SIZE = 32;

    UInt64 inline_visited[INLINE_VISITED_WORDS];
    VectorWithMemoryTracking<UInt64> heap_visited;
    UInt64 * visited = inline_visited;
    if (num_words <= INLINE_VISITED_WORDS)
    {
        std::memset(inline_visited, 0, num_words * sizeof(UInt64));
    }
    else
    {
        heap_visited.assign(num_words, 0);
        visited = heap_visited.data();
    }

    struct State
    {
        size_t pc;
        size_t pos;
    };

    State inline_stack[INLINE_STACK_SIZE];
    size_t inline_stack_size = 0;
    VectorWithMemoryTracking<State> heap_stack;

    /// Returns false if the state has been visited already.
    auto visit = [&](size_t pc, size_t pos)
    {
        const size_t state = pc * num_positions + pos;
        const UInt64 mask = UInt64(1) << (state % 64);
        if (visited[state / 64] & mask)
            return false;
        visited[state / 64] |= mask;
        return true;
    };

    auto push = [&](size_t pc, size_t pos)
    {
        if (!visit(pc, pos))
            return;

        if (inline_stack_size < INLINE_STACK_SIZE)
            inline_stack[inline_stack_size++] = State{pc, pos};
        else
            heap_stack.push_back(State{pc, pos});
    };

    const std::string_view text(pattern);

    push(0, 0);

    while (inline_stack_size > 0)
    {
        State state{};
        if (!heap_stack.empty())
        {
            state = heap_stack.back();
            heap_stack.pop_back();
        }
        else
        {
            state = inline_stack[--inline_stack_size];
        }

        const size_t pc = state.pc;
        const size_t pos = state.pos;
        const Op & op = program[pc];

        switch (op.type)
        {
            case OpType::Literal:
            {
                if (candidate.substr(pos).starts_with(text.substr(op.first, op.second)))
                    push(pc + 1, pos + op.second);
                break;
            }
            case OpType::AnyOne:
            {
                if (pos == candidate.size() || candidate[pos] == '/')
                    break;
                if (const size_t length = characterLength(candidate, pos))
                    push(pc + 1, pos + length);
                break;
            }
            case OpType::Star:
            case OpType::StarNoBrace:
            {
                auto is_excluded = [&](char c) { return op.type == OpType::Star ? c == '/' : (c == '{' || c == '}'); };

                /// What follows the star often tells where it may end without trying: a literal has
                /// to begin with its first byte, and the end of the pattern is the end of the candidate.
                const Op & next = program[pc + 1];
                const bool ends_candidate = next.type == OpType::Match || next.type == OpType::AssertEnd;
                const bool ends_at_byte = next.type == OpType::Literal;

                if (!ends_candidate && !ends_at_byte)
                {
                    if (pos < candidate.size() && !is_excluded(candidate[pos]))
                        if (const size_t length = characterLength(candidate, pos))
                            push(pc, pos + length);

                    /// Pushed last to be tried first: the shortest match first keeps the stack short.
                    push(pc + 1, pos);
                    break;
                }

                /// Walk over everything the star matches, and continue only from the places it may end at.
                /// The places walked over are marked as visited: the walk from any of them is a part of this one.
                size_t end = pos;
                while (true)
                {
                    if (ends_candidate ? end == candidate.size() : (end < candidate.size() && candidate[end] == text[next.first]))
                        push(pc + 1, end);

                    if (end == candidate.size() || is_excluded(candidate[end]))
                        break;

                    const size_t length = characterLength(candidate, end);
                    if (length == 0)
                        break;

                    end += length;
                    if (!visit(pc, end))
                        break;
                }
                break;
            }
            case OpType::Globstar:
            {
                /// One more directory: characters other than '/' up to a '/'.
                size_t end = pos;
                while (end < candidate.size() && candidate[end] != '/')
                {
                    const size_t length = characterLength(candidate, end);
                    if (length == 0)
                        break;
                    end += length;
                }
                if (end < candidate.size() && candidate[end] == '/')
                    push(pc, end + 1);

                push(pc + 1, pos);
                break;
            }
            case OpType::AssertBegin:
            {
                if (pos == 0)
                    push(pc + 1, pos);
                break;
            }
            case OpType::AssertEnd:
            {
                if (pos == candidate.size())
                    push(pc + 1, pos);
                break;
            }
            case OpType::Range:
            {
                const Range & range = ranges[op.first];

                if (range.width > 0)
                {
                    if (candidate.size() - pos < range.width)
                        break;

                    UInt64 value = 0;
                    bool fits = true;
                    for (size_t i = 0; i < range.width && fits; ++i)
                    {
                        const char c = candidate[pos + i];
                        fits = isDigit(c) && appendDigit(value, c) && value <= range.max;
                    }

                    if (fits && value >= range.min)
                        push(pc + 1, pos + range.width);
                    break;
                }

                if (pos == candidate.size() || !isDigit(candidate[pos]))
                    break;

                /// A number that is not padded does not start with '0', unless it is '0'.
                if (candidate[pos] == '0')
                {
                    if (range.min == 0)
                        push(pc + 1, pos + 1);
                    break;
                }

                UInt64 value = 0;
                for (size_t i = pos; i < candidate.size() && isDigit(candidate[i]); ++i)
                {
                    if (!appendDigit(value, candidate[i]) || value > range.max)
                        break;
                    if (value >= range.min)
                        push(pc + 1, i + 1);
                }
                break;
            }
            case OpType::AltLiteral:
            {
                const size_t remaining = candidate.size() - pos;

                for (size_t i = op.first; i < op.first + op.second; ++i)
                {
                    const LiteralGroup & group = literal_groups[i];

                    /// The groups are sorted by the length of their alternatives.
                    if (group.size > remaining)
                        break;

                    const std::string_view piece = candidate.substr(pos, group.size);
                    const LiteralAlternative * group_begin = literal_alternatives.data() + group.first;
                    const LiteralAlternative * group_end = group_begin + group.count;

                    const LiteralAlternative * it = std::lower_bound(group_begin, group_end, piece,
                        [&](const LiteralAlternative & alternative, std::string_view value)
                        {
                            return text.substr(alternative.offset, alternative.size) < value;
                        });

                    if (it != group_end && text.substr(it->offset, it->size) == piece)
                        push(pc + 1, pos + group.size);
                }
                break;
            }
            case OpType::Alt:
            {
                for (size_t i = op.first + op.second; i > op.first; --i)
                    push(branch_targets[i - 1], pos);
                break;
            }
            case OpType::Jump:
            {
                push(op.first, pos);
                break;
            }
            case OpType::Match:
            {
                if (pos == candidate.size())
                    return true;
                break;
            }
        }
    }

    return false;
}

}
