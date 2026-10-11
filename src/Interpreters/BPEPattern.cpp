#include <Interpreters/BPEPattern.h>

#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>
#include <Common/checkStackSize.h>

#include <array>
#include <limits>
#include <optional>

#include "config.h"

#if USE_ICU
#    include <unicode/uchar.h>

/// ICU wraps every entry point in a `U_ICU_ENTRY_POINT_RENAME(name)` macro that re-uses the original
/// name during expansion, so every ICU call below triggers `-Wdisabled-macro-expansion`.
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

#if USE_ICU

namespace
{

constexpr UInt32 invalid_code_point = 0xFFFFFFFFu;
constexpr size_t unbounded = std::numeric_limits<size_t>::max();

UInt32 decodeAt(std::string_view text, size_t pos, size_t & length)
{
    const auto byte = static_cast<UInt8>(text[pos]);
    if (byte < 0x80)
    {
        length = 1;
        return byte;
    }

    const size_t sequence_length = UTF8::seqLength(byte);
    if (sequence_length <= text.size() - pos)
    {
        if (auto code_point = UTF8::convertUTF8ToCodePoint(text.data() + pos, sequence_length))
        {
            length = sequence_length;
            return *code_point;
        }
    }
    length = 1;
    return invalid_code_point;
}

UInt32 foldCase(UInt32 code_point)
{
    if (code_point == invalid_code_point)
        return code_point;
    return static_cast<UInt32>(u_foldCase(static_cast<UChar32>(code_point), U_FOLD_CASE_DEFAULT));
}

/// The general categories `\p{...}` accepts, by their short and long names.
uint32_t categoryMask(std::string_view name)
{
    struct Category
    {
        std::string_view short_name;
        std::string_view long_name;
        uint32_t mask;
    };
    static constexpr std::array categories{
        Category{"L", "Letter", U_GC_L_MASK},
        Category{"Lu", "Uppercase_Letter", U_GC_LU_MASK},
        Category{"Ll", "Lowercase_Letter", U_GC_LL_MASK},
        Category{"Lt", "Titlecase_Letter", U_GC_LT_MASK},
        Category{"Lm", "Modifier_Letter", U_GC_LM_MASK},
        Category{"Lo", "Other_Letter", U_GC_LO_MASK},
        Category{"LC", "Cased_Letter", U_GC_LC_MASK},
        Category{"M", "Mark", U_GC_M_MASK},
        Category{"Mn", "Nonspacing_Mark", U_GC_MN_MASK},
        Category{"Mc", "Spacing_Mark", U_GC_MC_MASK},
        Category{"Me", "Enclosing_Mark", U_GC_ME_MASK},
        Category{"N", "Number", U_GC_N_MASK},
        Category{"Nd", "Decimal_Number", U_GC_ND_MASK},
        Category{"Nl", "Letter_Number", U_GC_NL_MASK},
        Category{"No", "Other_Number", U_GC_NO_MASK},
        Category{"P", "Punctuation", U_GC_P_MASK},
        Category{"Pc", "Connector_Punctuation", U_GC_PC_MASK},
        Category{"Pd", "Dash_Punctuation", U_GC_PD_MASK},
        Category{"Ps", "Open_Punctuation", U_GC_PS_MASK},
        Category{"Pe", "Close_Punctuation", U_GC_PE_MASK},
        Category{"Pi", "Initial_Punctuation", U_GC_PI_MASK},
        Category{"Pf", "Final_Punctuation", U_GC_PF_MASK},
        Category{"Po", "Other_Punctuation", U_GC_PO_MASK},
        Category{"S", "Symbol", U_GC_S_MASK},
        Category{"Sm", "Math_Symbol", U_GC_SM_MASK},
        Category{"Sc", "Currency_Symbol", U_GC_SC_MASK},
        Category{"Sk", "Modifier_Symbol", U_GC_SK_MASK},
        Category{"So", "Other_Symbol", U_GC_SO_MASK},
        Category{"Z", "Separator", U_GC_Z_MASK},
        Category{"Zs", "Space_Separator", U_GC_ZS_MASK},
        Category{"Zl", "Line_Separator", U_GC_ZL_MASK},
        Category{"Zp", "Paragraph_Separator", U_GC_ZP_MASK},
        Category{"C", "Other", U_GC_C_MASK},
        Category{"Cc", "Control", U_GC_CC_MASK},
        Category{"Cf", "Format", U_GC_CF_MASK},
        Category{"Cs", "Surrogate", U_GC_CS_MASK},
        Category{"Co", "Private_Use", U_GC_CO_MASK},
        Category{"Cn", "Unassigned", U_GC_CN_MASK},
    };

    for (const auto & category : categories)
        if (name == category.short_name || name == category.long_name)
            return category.mask;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "A pre-tokenizer pattern uses the property \\p{{{}}}, which is not supported; only general categories are", name);
}

}

/// A character class: any of its items, possibly negated as a whole.
struct BPEPattern::CharacterSet
{
    struct Item
    {
        enum class Kind : UInt8
        {
            Range,
            Category,
            Whitespace,
        };

        Kind kind = Kind::Range;
        /// For `\S`, `\P{...}` and the like, which hold for what the item does not.
        bool negated = false;
        UInt32 low = 0;
        UInt32 high = 0;
        uint32_t category_mask = 0;
    };

    std::vector<Item> items;
    bool negated = false;
    /// Under `(?i:...)` a character is also tried in its case folded form, and a literal is added in
    /// both of its forms, so that `'s` and `'S` and `'ſ` all match `(?i:'s)`.
    bool case_insensitive = false;

    /// Every character of the text is tested against a class, so the Basic Multilingual Plane is
    /// tested once, when the pattern is compiled.
    std::array<UInt64, 0x10000 / 64> bmp{};

    bool itemsContain(UInt32 code_point) const
    {
        for (const auto & item : items)
        {
            bool in = false;
            if (code_point != invalid_code_point)
            {
                const auto c = static_cast<UChar32>(code_point);
                switch (item.kind)
                {
                    case Item::Kind::Range:
                        in = code_point >= item.low && code_point <= item.high;
                        break;
                    case Item::Kind::Category:
                        in = (U_GET_GC_MASK(c) & item.category_mask) != 0;
                        break;
                    case Item::Kind::Whitespace:
                        in = u_hasBinaryProperty(c, UCHAR_WHITE_SPACE);
                        break;
                }
            }
            if (in != item.negated)
                return true;
        }
        return false;
    }

    bool computeContains(UInt32 code_point) const
    {
        bool in = itemsContain(code_point);
        if (!in && case_insensitive)
            in = itemsContain(foldCase(code_point));
        return in != negated;
    }

    void finalize()
    {
        for (UInt32 c = 0; c < 0x10000; ++c)
            if (computeContains(c))
                bmp[c / 64] |= UInt64{1} << (c % 64);
    }

    bool contains(UInt32 code_point) const
    {
        if (code_point < 0x10000)
            return ((bmp[code_point / 64] >> (code_point % 64)) & UInt64{1}) != 0;
        return computeContains(code_point);
    }

    void addLiteral(UInt32 code_point)
    {
        items.push_back({Item::Kind::Range, false, code_point, code_point, 0});
        if (case_insensitive)
        {
            const UInt32 folded = foldCase(code_point);
            items.push_back({Item::Kind::Range, false, folded, folded, 0});
        }
    }
};

struct BPEPattern::Node
{
    enum class Kind : UInt8
    {
        Set,
        Sequence,
        Alternation,
        Repeat,
        LookAhead,
        TextBegin,
        TextEnd,
    };

    enum class Mode : UInt8
    {
        Greedy,
        Lazy,
        Possessive,
    };

    Kind kind = Kind::Sequence;
    const CharacterSet * set = nullptr;
    std::vector<std::unique_ptr<Node>> children;
    /// Of a `Repeat`.
    size_t min = 0;
    size_t max = 0;
    Mode mode = Mode::Greedy;
    /// Of a `LookAhead`: `(?!...)` rather than `(?=...)`.
    bool negated = false;
};

/// What is left to match once a node has matched: the rest of a sequence, or the rest of a
/// repetition. A stack of these, on the call stack, is the backtracking state.
struct BPEPattern::Continuation
{
    const Node * node = nullptr;
    const Continuation * next = nullptr;
    /// Of a `Sequence`, the child to match next; of a `Repeat`, the iterations done so far.
    size_t index = 0;
    /// Of a `Repeat`, where the iteration that has just finished started.
    size_t iteration_start = 0;
};

namespace
{

class Parser
{
public:
    using Node = BPEPattern::Node;
    using CharacterSet = BPEPattern::CharacterSet;

    Parser(std::string_view pattern_, std::vector<std::unique_ptr<CharacterSet>> & sets_)
        : pattern(pattern_), sets(sets_)
    {
    }

    std::unique_ptr<Node> parse()
    {
        auto node = parseAlternation(/*case_insensitive=*/false);
        if (pos != pattern.size())
            fail("an unbalanced ')'");
        return node;
    }

private:
    std::string_view pattern;
    std::vector<std::unique_ptr<CharacterSet>> & sets;
    size_t pos = 0;

    [[noreturn]] void fail(std::string_view what) const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot compile the pre-tokenizer pattern '{}': {} at position {}", pattern, what, pos);
    }

    bool atEnd() const { return pos >= pattern.size(); }
    char peek() const { return atEnd() ? '\0' : pattern[pos]; }

    bool consume(std::string_view prefix)
    {
        if (pattern.substr(pos).starts_with(prefix))
        {
            pos += prefix.size();
            return true;
        }
        return false;
    }

    UInt32 nextCodePoint()
    {
        size_t length = 0;
        const UInt32 code_point = decodeAt(pattern, pos, length);
        if (code_point == invalid_code_point)
            fail("a byte that is not valid UTF-8");
        pos += length;
        return code_point;
    }

    CharacterSet & newSet(bool case_insensitive)
    {
        sets.push_back(std::make_unique<CharacterSet>());
        sets.back()->case_insensitive = case_insensitive;
        return *sets.back();
    }

    static std::unique_ptr<Node> setNode(const CharacterSet & set)
    {
        auto node = std::make_unique<Node>();
        node->kind = Node::Kind::Set;
        node->set = &set;
        return node;
    }

    std::unique_ptr<Node> parseAlternation(bool case_insensitive)
    {
        std::vector<std::unique_ptr<Node>> branches;
        branches.push_back(parseSequence(case_insensitive));
        while (peek() == '|')
        {
            ++pos;
            branches.push_back(parseSequence(case_insensitive));
        }
        if (branches.size() == 1)
            return std::move(branches.front());

        auto node = std::make_unique<Node>();
        node->kind = Node::Kind::Alternation;
        node->children = std::move(branches);
        return node;
    }

    std::unique_ptr<Node> parseSequence(bool case_insensitive)
    {
        auto node = std::make_unique<Node>();
        node->kind = Node::Kind::Sequence;
        while (!atEnd() && peek() != '|' && peek() != ')')
        {
            auto atom = parseAtom(case_insensitive);
            node->children.push_back(parseQuantifier(std::move(atom)));
        }
        if (node->children.size() == 1)
            return std::move(node->children.front());
        return node;
    }

    std::unique_ptr<Node> parseAtom(bool case_insensitive)
    {
        const char c = peek();
        switch (c)
        {
            case '(':
            {
                ++pos;
                std::unique_ptr<Node> node;
                if (consume("?:"))
                    node = parseAlternation(case_insensitive);
                else if (consume("?i:"))
                    node = parseAlternation(/*case_insensitive=*/true);
                else if (consume("?=") || consume("?!"))
                {
                    const bool negated = pattern[pos - 1] == '!';
                    node = std::make_unique<Node>();
                    node->kind = Node::Kind::LookAhead;
                    node->negated = negated;
                    node->children.push_back(parseAlternation(case_insensitive));
                }
                else if (peek() == '?')
                    fail("a group construct that is not supported");
                else
                    node = parseAlternation(case_insensitive);

                if (!consume(")"))
                    fail("a missing ')'");
                return node;
            }
            case '[':
            {
                ++pos;
                return setNode(parseClass(case_insensitive));
            }
            case '.':
            {
                ++pos;
                auto & set = newSet(false);
                set.negated = true;
                set.addLiteral('\n');
                set.finalize();
                return setNode(set);
            }
            case '^':
            case '$':
            {
                ++pos;
                auto node = std::make_unique<Node>();
                node->kind = c == '^' ? Node::Kind::TextBegin : Node::Kind::TextEnd;
                return node;
            }
            case '*':
            case '+':
            case '?':
            case '{':
                fail("a quantifier with nothing to repeat");
            case '\\':
            {
                ++pos;
                auto & set = newSet(case_insensitive);
                parseEscape(set);
                set.finalize();
                return setNode(set);
            }
            default:
            {
                auto & set = newSet(case_insensitive);
                set.addLiteral(nextCodePoint());
                set.finalize();
                return setNode(set);
            }
        }
    }

    std::unique_ptr<Node> parseQuantifier(std::unique_ptr<Node> atom)
    {
        size_t min = 0;
        size_t max = 0;
        switch (peek())
        {
            case '?': min = 0; max = 1; ++pos; break;
            case '*': min = 0; max = unbounded; ++pos; break;
            case '+': min = 1; max = unbounded; ++pos; break;
            case '{':
            {
                ++pos;
                min = parseNumber();
                max = min;
                if (consume(","))
                    max = peek() == '}' ? unbounded : parseNumber();
                if (!consume("}"))
                    fail("a malformed {n,m} quantifier");
                if (max < min)
                    fail("a {n,m} quantifier with m below n");
                break;
            }
            default:
                return atom;
        }

        if (atom->kind == Node::Kind::TextBegin || atom->kind == Node::Kind::TextEnd || atom->kind == Node::Kind::LookAhead)
            fail("a quantifier on an assertion");

        auto node = std::make_unique<Node>();
        node->kind = Node::Kind::Repeat;
        node->min = min;
        node->max = max;
        if (consume("?"))
            node->mode = Node::Mode::Lazy;
        else if (consume("+"))
            node->mode = Node::Mode::Possessive;
        node->children.push_back(std::move(atom));

        if (peek() == '*' || peek() == '+' || peek() == '?' || peek() == '{')
            fail("two quantifiers in a row");
        return node;
    }

    size_t parseNumber()
    {
        size_t value = 0;
        const size_t start = pos;
        while (!atEnd() && peek() >= '0' && peek() <= '9')
        {
            value = value * 10 + static_cast<size_t>(peek() - '0');
            if (value > 100000)
                fail("a repetition count that is too large");
            ++pos;
        }
        if (pos == start)
            fail("a missing number in a {n,m} quantifier");
        return value;
    }

    UInt32 parseHex(size_t digits)
    {
        UInt32 value = 0;
        for (size_t i = 0; i < digits; ++i)
        {
            const char c = peek();
            UInt32 digit = 0;
            if (c >= '0' && c <= '9')
                digit = static_cast<UInt32>(c - '0');
            else if (c >= 'a' && c <= 'f')
                digit = static_cast<UInt32>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F')
                digit = static_cast<UInt32>(c - 'A' + 10);
            else
                fail("a malformed hexadecimal escape");
            value = value * 16 + digit;
            ++pos;
        }
        return value;
    }

    /// After a backslash. A class shorthand adds its item to `set`; anything else is a literal,
    /// which is returned, so that a class can use it as the end of a range.
    std::optional<UInt32> parseEscapeItem(CharacterSet & set)
    {
        using Item = CharacterSet::Item;
        if (atEnd())
            fail("a trailing backslash");

        const char c = pattern[pos++];
        switch (c)
        {
            case 's': set.items.push_back({Item::Kind::Whitespace, false, 0, 0, 0}); return {};
            case 'S': set.items.push_back({Item::Kind::Whitespace, true, 0, 0, 0}); return {};
            case 'd': set.items.push_back({Item::Kind::Category, false, 0, 0, U_GC_ND_MASK}); return {};
            case 'D': set.items.push_back({Item::Kind::Category, true, 0, 0, U_GC_ND_MASK}); return {};
            case 'w':
            case 'W':
            {
                const uint32_t word = U_GC_L_MASK | U_GC_M_MASK | U_GC_N_MASK | U_GC_PC_MASK;
                set.items.push_back({Item::Kind::Category, c == 'W', 0, 0, word});
                return {};
            }
            case 'p':
            case 'P':
            {
                if (!consume("{"))
                    fail("a property escape without '{'");
                bool negated = c == 'P';
                if (consume("^"))
                    negated = !negated;
                const size_t name_end = pattern.find('}', pos);
                if (name_end == std::string_view::npos)
                    fail("a property escape without '}'");
                const uint32_t mask = categoryMask(pattern.substr(pos, name_end - pos));
                pos = name_end + 1;
                set.items.push_back({Item::Kind::Category, negated, 0, 0, mask});
                return {};
            }
            case 'r': return '\r';
            case 'n': return '\n';
            case 't': return '\t';
            case 'f': return '\f';
            case 'v': return '\v';
            case 'a': return '\a';
            case 'e': return 0x1B;
            case '0': return 0;
            case 'x':
            {
                if (consume("{"))
                {
                    const size_t start = pos;
                    UInt32 value = 0;
                    while (!atEnd() && peek() != '}')
                    {
                        value = value * 16 + parseHex(1);
                        if (pos - start > 6)
                            fail("a hexadecimal escape that is too long");
                    }
                    if (!consume("}"))
                        fail("a hexadecimal escape without '}'");
                    return value;
                }
                return parseHex(2);
            }
            case 'u': return parseHex(4);
            default:
                break;
        }

        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
            fail("an escape that is not supported");

        /// An escaped punctuation character stands for itself, and so does an escaped non-ASCII one.
        --pos;
        return nextCodePoint();
    }

    void parseEscape(CharacterSet & set)
    {
        if (auto literal = parseEscapeItem(set))
            set.addLiteral(*literal);
    }

    const CharacterSet & parseClass(bool case_insensitive)
    {
        auto & set = newSet(case_insensitive);
        if (consume("^"))
            set.negated = true;

        bool first = true;
        while (true)
        {
            if (atEnd())
                fail("an unterminated character class");
            if (peek() == ']' && !first)
            {
                ++pos;
                break;
            }
            if (peek() == '[')
                fail("a nested character class, which is not supported");
            if (pattern.substr(pos).starts_with("&&"))
                fail("a character class intersection, which is not supported");
            first = false;

            std::optional<UInt32> low;
            if (consume("\\"))
                low = parseEscapeItem(set);
            else
                low = nextCodePoint();

            if (!low)
                continue;

            /// A '-' between two characters makes a range; at the start or the end it is itself.
            if (peek() == '-' && pos + 1 < pattern.size() && pattern[pos + 1] != ']')
            {
                ++pos;
                std::optional<UInt32> high;
                if (consume("\\"))
                {
                    high = parseEscapeItem(set);
                    if (!high)
                        fail("a range that ends in a class shorthand");
                }
                else
                    high = nextCodePoint();

                if (*high < *low)
                    fail("a range whose end is below its start");
                /// Under `(?i:...)` a range is taken as written, and the text is folded against it.
                set.items.push_back({CharacterSet::Item::Kind::Range, false, *low, *high, 0});
            }
            else
                set.addLiteral(*low);
        }

        set.finalize();
        return set;
    }
};

}

BPEPattern::BPEPattern(std::string_view pattern)
{
    root = Parser(pattern, sets).parse();
}

BPEPattern::~BPEPattern() = default;

bool BPEPattern::matchNext(std::string_view text, size_t pos, const Continuation * next, size_t & end) const
{
    if (!next)
    {
        end = pos;
        return true;
    }

    const Node & node = *next->node;
    if (node.kind == Node::Kind::Sequence)
    {
        if (next->index == node.children.size())
            return matchNext(text, pos, next->next, end);
        const Continuation rest{&node, next->next, next->index + 1, 0};
        return matchNode(*node.children[next->index], text, pos, &rest, end);
    }

    /// An iteration of a repetition has just matched. One that matched nothing would match nothing
    /// again, so the repetition ends there.
    chassert(node.kind == Node::Kind::Repeat);
    if (pos == next->iteration_start)
        return matchNext(text, pos, next->next, end);
    return matchRepeat(node, text, pos, next->index, next->next, end);
}

bool BPEPattern::matchRepeat(
    const Node & node, std::string_view text, size_t pos, size_t count, const Continuation * next, size_t & end) const
{
    const Node & child = *node.children.front();
    const Continuation again{&node, next, count + 1, pos};

    if (node.mode == Node::Mode::Lazy)
    {
        if (count >= node.min && matchNext(text, pos, next, end))
            return true;
        return count < node.max && matchNode(child, text, pos, &again, end);
    }

    if (count < node.max && matchNode(child, text, pos, &again, end))
        return true;
    return count >= node.min && matchNext(text, pos, next, end);
}

bool BPEPattern::matchNode(const Node & node, std::string_view text, size_t pos, const Continuation * next, size_t & end) const
{
    checkStackSize();

    switch (node.kind)
    {
        case Node::Kind::Set:
        {
            if (pos >= text.size())
                return false;
            size_t length = 0;
            if (!node.set->contains(decodeAt(text, pos, length)))
                return false;
            return matchNext(text, pos + length, next, end);
        }

        case Node::Kind::Sequence:
        {
            if (node.children.empty())
                return matchNext(text, pos, next, end);
            const Continuation rest{&node, next, 1, 0};
            return matchNode(*node.children.front(), text, pos, &rest, end);
        }

        case Node::Kind::Alternation:
        {
            for (const auto & branch : node.children)
                if (matchNode(*branch, text, pos, next, end))
                    return true;
            return false;
        }

        case Node::Kind::LookAhead:
        {
            size_t lookahead_end = 0;
            const bool found = matchNode(*node.children.front(), text, pos, nullptr, lookahead_end);
            if (found == node.negated)
                return false;
            return matchNext(text, pos, next, end);
        }

        case Node::Kind::TextBegin:
            return pos == 0 && matchNext(text, pos, next, end);

        case Node::Kind::TextEnd:
            return pos == text.size() && matchNext(text, pos, next, end);

        case Node::Kind::Repeat:
        {
            const Node & child = *node.children.front();

            /// A repetition of a single character, which is what these patterns are made of, is
            /// run as a loop, so that a long run of letters does not become a deep recursion.
            if (child.kind == Node::Kind::Set)
            {
                if (node.mode == Node::Mode::Lazy)
                {
                    size_t count = 0;
                    while (true)
                    {
                        if (count >= node.min && matchNext(text, pos, next, end))
                            return true;
                        if (count == node.max || pos >= text.size())
                            return false;
                        size_t length = 0;
                        if (!child.set->contains(decodeAt(text, pos, length)))
                            return false;
                        pos += length;
                        ++count;
                    }
                }

                PODArrayWithStackMemory<size_t, 64 * sizeof(size_t)> positions;
                positions.push_back(pos);
                while (positions.size() - 1 < node.max && pos < text.size())
                {
                    size_t length = 0;
                    if (!child.set->contains(decodeAt(text, pos, length)))
                        break;
                    pos += length;
                    positions.push_back(pos);
                }

                const size_t matched = positions.size() - 1;
                if (matched < node.min)
                    return false;
                if (node.mode == Node::Mode::Possessive)
                    return matchNext(text, positions.back(), next, end);

                for (size_t count = matched + 1; count-- > node.min;)
                    if (matchNext(text, positions[count], next, end))
                        return true;
                return false;
            }

            if (node.mode == Node::Mode::Possessive)
            {
                /// The repetition on its own, committed to its first match.
                size_t repeat_end = 0;
                if (!matchRepeat(node, text, pos, 0, nullptr, repeat_end))
                    return false;
                return matchNext(text, repeat_end, next, end);
            }

            return matchRepeat(node, text, pos, 0, next, end);
        }
    }
}

bool BPEPattern::find(std::string_view text, size_t from, size_t & match_begin, size_t & match_end) const
{
    for (size_t start = from; start <= text.size();)
    {
        if (matchNode(*root, text, start, nullptr, match_end))
        {
            match_begin = start;
            return true;
        }
        if (start == text.size())
            break;
        size_t length = 0;
        decodeAt(text, start, length);
        start += length;
    }
    return false;
}

#else

BPEPattern::BPEPattern(std::string_view)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Pre-tokenizer patterns require ClickHouse to be built with ICU");
}

BPEPattern::~BPEPattern() = default;

bool BPEPattern::find(std::string_view, size_t, size_t &, size_t &) const
{
    return false;
}

#endif

}

#if USE_ICU
#    pragma clang diagnostic pop
#endif
