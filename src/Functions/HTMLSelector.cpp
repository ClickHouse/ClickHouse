#include <Functions/HTMLSelector.h>

#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <base/find_symbols.h>

#include <algorithm>
#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace HTML
{

namespace
{

inline bool equalsCaseInsensitive(std::string_view a, std::string_view b)
{
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (toLowerASCII(a[i]) != toLowerASCII(b[i]))
            return false;
    return true;
}

inline bool equalsMaybeCaseInsensitive(std::string_view a, std::string_view b, bool case_insensitive)
{
    return case_insensitive ? equalsCaseInsensitive(a, b) : a == b;
}

String toLowerCopy(std::string_view s)
{
    String res(s);
    toLowerASCII(res);
    return res;
}

bool containsMaybeCaseInsensitive(std::string_view haystack, std::string_view needle, bool case_insensitive)
{
    if (!case_insensitive)
        return haystack.contains(needle);
    if (needle.size() > haystack.size())
        return false;
    for (size_t i = 0; i + needle.size() <= haystack.size(); ++i)
        if (equalsCaseInsensitive(haystack.substr(i, needle.size()), needle))
            return true;
    return false;
}

/// Whitespace in the sense of HTML and CSS.
inline bool isHTMLWhitespace(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
}

}


bool AttributeCondition::matches(std::string_view attribute_value) const
{
    switch (op)
    {
        case Operator::Exists:
            return true;
        case Operator::Equals:
            return equalsMaybeCaseInsensitive(attribute_value, value, case_insensitive);
        case Operator::Includes:
        {
            /// A whitespace-separated list of words, one of which is exactly the value.
            const char * pos = attribute_value.data();
            const char * end = pos + attribute_value.size();
            while (pos < end)
            {
                while (pos < end && isHTMLWhitespace(*pos))
                    ++pos;
                const char * word_begin = pos;
                while (pos < end && !isHTMLWhitespace(*pos))
                    ++pos;
                if (pos > word_begin
                    && equalsMaybeCaseInsensitive(std::string_view(word_begin, pos - word_begin), value, case_insensitive))
                    return true;
            }
            return false;
        }
        case Operator::DashMatch:
            return equalsMaybeCaseInsensitive(attribute_value, value, case_insensitive)
                || (attribute_value.size() > value.size() && attribute_value[value.size()] == '-'
                    && equalsMaybeCaseInsensitive(attribute_value.substr(0, value.size()), value, case_insensitive));
        case Operator::Prefix:
            return attribute_value.size() >= value.size()
                && equalsMaybeCaseInsensitive(attribute_value.substr(0, value.size()), value, case_insensitive);
        case Operator::Suffix:
            return attribute_value.size() >= value.size()
                && equalsMaybeCaseInsensitive(attribute_value.substr(attribute_value.size() - value.size()), value, case_insensitive);
        case Operator::Substring:
            return containsMaybeCaseInsensitive(attribute_value, value, case_insensitive);
    }
}


bool NthCondition::matches(UInt64 index) const
{
    Int64 n = static_cast<Int64>(index);
    if (a == 0)
        return n == b;
    Int64 diff = n - b;
    return diff % a == 0 && diff / a >= 0;
}


bool CompoundSelector::needsAttributes() const
{
    if (!attributes.empty())
        return true;
    for (const auto & negation : negations)
        if (negation.needsAttributes())
            return true;
    return false;
}


const Attribute * ElementInfo::findAttribute(std::string_view lowercase_name) const
{
    /// If an attribute is repeated, the first occurrence is used, as in HTML.
    for (const auto & attribute : *attributes)
        if (equalsCaseInsensitive(attribute.name, lowercase_name))
            return &attribute;
    return nullptr;
}


bool matchCompound(const CompoundSelector & compound, const ElementInfo & element)
{
    if (!compound.tag.empty() && !equalsCaseInsensitive(element.name, compound.tag))
        return false;

    if (compound.root && !element.is_root)
        return false;

    for (const auto & nth : compound.nth_child)
        if (!nth.matches(element.index))
            return false;

    for (const auto & condition : compound.attributes)
    {
        const Attribute * attribute = element.findAttribute(condition.name);
        if (!attribute || !condition.matches(attribute->value))
            return false;
    }

    for (const auto & negation : compound.negations)
        if (matchCompound(negation, element))
            return false;

    return true;
}


/// Parser of CSS selectors.
/// Grammar (a subset of CSS Selectors Level 3 and 4):
///
///   selector_list   := complex (',' complex)*
///   complex         := compound (combinator compound)* pseudo_element?
///   combinator      := '>' | '+' | '~' | whitespace
///   compound        := (type | '*')? (id | class | attribute | pseudo_class)*
///   id              := '#' name
///   class           := '.' name
///   attribute       := '[' name (('=' | '~=' | '|=' | '^=' | '$=' | '*=') (name | string) ('i' | 's')?)? ']'
///   pseudo_class    := ':first-child' | ':nth-child(' an+b ')' | ':not(' compound (',' compound)* ')' | ':root'
///   pseudo_element  := '::text' | '::attr(' name ')'
namespace
{

class SelectorParser
{
public:
    explicit SelectorParser(std::string_view text_)
        : text(text_), pos(text.data()), end(text.data() + text.size())
    {
    }

    void parse(Selector & selector)
    {
        skipWhitespace();
        if (pos == end)
            throwError("the selector is empty");

        while (true)
        {
            parseComplex(selector);
            if (pos == end)
                break;
            if (*pos != ',')
                throwError("expected a comma or the end of the selector");
            ++pos;
            skipWhitespace();
        }
    }

private:
    std::string_view text;
    const char * pos;
    const char * end;

    [[noreturn]] void throwError(std::string_view message) const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse CSS selector '{}' at position {}: {}",
            text, pos - text.data() + 1, message);
    }

    bool skipWhitespace()
    {
        const char * old_pos = pos;
        while (pos < end && isHTMLWhitespace(*pos))
            ++pos;
        return pos != old_pos;
    }

    [[nodiscard]] bool checkChar(char c) const { return pos < end && *pos == c; }

    void expectChar(char c)
    {
        if (!checkChar(c))
            throwError(fmt::format("expected '{}'", c));
        ++pos;
    }

    static bool isNameChar(char c)
    {
        return isAlphaNumericASCII(c) || c == '-' || c == '_' || static_cast<unsigned char>(c) >= 0x80;
    }

    [[nodiscard]] bool isNameStart() const
    {
        return pos < end && (isNameChar(*pos) || *pos == '\\');
    }

    void parseEscape(String & out)
    {
        ++pos; /// backslash
        if (pos == end)
            throwError("unterminated escape sequence");

        if (isHexDigit(*pos))
        {
            UInt32 code_point = 0;
            for (size_t i = 0; i < 6 && pos < end && isHexDigit(*pos); ++i, ++pos)
                code_point = code_point * 16 + unhex(*pos);
            /// A single whitespace after a hexadecimal escape is a part of it.
            if (pos < end && isHTMLWhitespace(*pos))
                ++pos;
            if (code_point == 0 || code_point > 0x10FFFF || (code_point >= 0xD800 && code_point <= 0xDFFF))
                code_point = 0xFFFD;
            char buf[4];
            size_t size = UTF8::convertCodePointToUTF8(code_point, buf, sizeof(buf));
            out.append(buf, size);
        }
        else if (*pos == '\n' || *pos == '\r' || *pos == '\f')
        {
            throwError("a newline cannot be escaped in a name");
        }
        else
        {
            out += *pos;
            ++pos;
        }
    }

    /// An identifier or a name (CSS allows names to start with digits in `#id`, we allow it everywhere).
    String parseName()
    {
        String res;
        while (pos < end)
        {
            if (*pos == '\\')
                parseEscape(res);
            else if (isNameChar(*pos))
                res += *pos++;
            else
                break;
        }
        if (res.empty())
            throwError("expected a name");
        return res;
    }

    String parseString()
    {
        char quote = *pos;
        ++pos;
        String res;
        while (true)
        {
            if (pos == end)
                throwError("unterminated string");
            if (*pos == quote)
            {
                ++pos;
                break;
            }
            if (*pos == '\\')
            {
                if (pos + 1 < end && (pos[1] == '\n' || pos[1] == '\r' || pos[1] == '\f'))
                {
                    /// An escaped newline in a string is a line continuation.
                    pos += 2;
                    continue;
                }
                parseEscape(res);
                continue;
            }
            res += *pos++;
        }
        return res;
    }

    void parseAttribute(CompoundSelector & compound)
    {
        ++pos; /// [
        skipWhitespace();

        AttributeCondition condition;
        if (checkChar('|') || checkChar('*'))
            throwError("namespaces are not supported");
        condition.name = toLowerCopy(parseName());
        skipWhitespace();
        if (checkChar('|') && !(pos + 1 < end && pos[1] == '='))
            throwError("namespaces are not supported");

        if (checkChar(']'))
        {
            ++pos;
            compound.attributes.push_back(std::move(condition));
            return;
        }

        using Operator = AttributeCondition::Operator;
        if (checkChar('='))
        {
            condition.op = Operator::Equals;
            ++pos;
        }
        else
        {
            if (pos + 1 >= end || pos[1] != '=')
                throwError("expected an attribute selector operator: =, ~=, |=, ^=, $=, *=");
            switch (*pos)
            {
                case '~': condition.op = Operator::Includes; break;
                case '|': condition.op = Operator::DashMatch; break;
                case '^': condition.op = Operator::Prefix; break;
                case '$': condition.op = Operator::Suffix; break;
                case '*': condition.op = Operator::Substring; break;
                default: throwError("expected an attribute selector operator: =, ~=, |=, ^=, $=, *=");
            }
            pos += 2;
        }

        skipWhitespace();
        if (checkChar('"') || checkChar('\''))
            condition.value = parseString();
        else
            condition.value = parseName();

        bool has_whitespace = skipWhitespace();
        if (has_whitespace && pos < end && (*pos == 'i' || *pos == 'I' || *pos == 's' || *pos == 'S'))
        {
            condition.case_insensitive = (*pos == 'i' || *pos == 'I');
            ++pos;
            skipWhitespace();
        }

        expectChar(']');

        /// The selectors that cannot match anything, according to the standard.
        bool never_matches = (condition.value.empty()
                && (condition.op == Operator::Prefix || condition.op == Operator::Suffix
                    || condition.op == Operator::Substring || condition.op == Operator::Includes))
            || (condition.op == Operator::Includes
                && std::any_of(condition.value.begin(), condition.value.end(), isHTMLWhitespace));

        if (never_matches)
            compound.negations.push_back(CompoundSelector{});    /// :not(*)

        compound.attributes.push_back(std::move(condition));
    }

    Int64 parseInteger()
    {
        if (pos == end || !isNumericASCII(*pos))
            throwError("expected a number");
        Int64 res = 0;
        while (pos < end && isNumericASCII(*pos))
        {
            res = res * 10 + (*pos - '0');
            if (res > 1'000'000'000)
                throwError("the number is too large");
            ++pos;
        }
        return res;
    }

    /// The argument of :nth-child: odd, even, or `a n + b`.
    NthCondition parseNth()
    {
        skipWhitespace();
        NthCondition res;

        if (pos < end && isAlphaASCII(*pos) && toLowerASCII(*pos) != 'n')
        {
            String keyword = toLowerCopy(parseName());
            if (keyword == "odd")
                res = {2, 1};
            else if (keyword == "even")
                res = {2, 0};
            else
                throwError("expected odd, even or an expression an+b");
        }
        else
        {
            bool negative = false;
            if (checkChar('+') || checkChar('-'))
            {
                negative = *pos == '-';
                ++pos;
            }

            bool has_number = pos < end && isNumericASCII(*pos);
            Int64 number = has_number ? parseInteger() : 1;
            if (negative)
                number = -number;

            if (pos < end && toLowerASCII(*pos) == 'n')
            {
                ++pos;
                res.a = number;
                res.b = 0;
                skipWhitespace();
                if (checkChar('+') || checkChar('-'))
                {
                    bool negative_b = *pos == '-';
                    ++pos;
                    skipWhitespace();
                    res.b = parseInteger();
                    if (negative_b)
                        res.b = -res.b;
                }
            }
            else
            {
                if (!has_number)
                    throwError("expected odd, even or an expression an+b");
                res.a = 0;
                res.b = number;
            }
        }

        skipWhitespace();
        expectChar(')');
        return res;
    }

    void parsePseudoClass(CompoundSelector & compound, bool inside_negation)
    {
        ++pos; /// :
        String name = toLowerCopy(parseName());

        if (name == "first-child")
        {
            compound.nth_child.push_back({0, 1});
        }
        else if (name == "root")
        {
            compound.root = true;
        }
        else if (name == "nth-child")
        {
            expectChar('(');
            compound.nth_child.push_back(parseNth());
        }
        else if (name == "not")
        {
            if (inside_negation)
                throwError("nested :not is not supported");
            expectChar('(');
            while (true)
            {
                skipWhitespace();
                compound.negations.push_back(parseCompound(true));
                skipWhitespace();
                if (checkChar(','))
                {
                    ++pos;
                    continue;
                }
                if (checkChar(')'))
                {
                    ++pos;
                    break;
                }
                throwError("only compound selectors (without combinators) are supported inside :not");
            }
        }
        else
        {
            throwError(fmt::format("unsupported pseudo-class :{}, the supported pseudo-classes are"
                " :first-child, :nth-child, :not, :root", name));
        }
    }

    CompoundSelector parseCompound(bool inside_negation)
    {
        CompoundSelector compound;
        bool has_something = false;

        if (checkChar('*'))
        {
            ++pos;
            has_something = true;
        }
        else if (isNameStart())
        {
            compound.tag = toLowerCopy(parseName());
            has_something = true;
        }

        if (checkChar('|'))
            throwError("namespaces are not supported");

        while (pos < end)
        {
            if (*pos == '#')
            {
                ++pos;
                compound.attributes.push_back({"id", parseName(), AttributeCondition::Operator::Equals});
            }
            else if (*pos == '.')
            {
                ++pos;
                compound.attributes.push_back({"class", parseName(), AttributeCondition::Operator::Includes});
            }
            else if (*pos == '[')
            {
                parseAttribute(compound);
            }
            else if (*pos == ':')
            {
                if (pos + 1 < end && pos[1] == ':')
                    break;  /// Pseudo-element.
                parsePseudoClass(compound, inside_negation);
            }
            else
                break;

            has_something = true;
        }

        if (!has_something)
            throwError("expected a compound selector");

        return compound;
    }

    void parsePseudoElement(Selector::ComplexSelectorOutput & output)
    {
        pos += 2; /// ::
        String name = toLowerCopy(parseName());
        if (name == "text")
        {
            output.output = Output::Text;
        }
        else if (name == "attr")
        {
            expectChar('(');
            skipWhitespace();
            output.output = Output::Attribute;
            output.attribute = toLowerCopy(parseName());
            skipWhitespace();
            expectChar(')');
        }
        else
        {
            throwError(fmt::format("unsupported pseudo-element ::{}, the supported pseudo-elements are ::text and ::attr(name)", name));
        }
    }

    void parseComplex(Selector & selector)
    {
        size_t selector_index = selector.outputs.size();
        Selector::ComplexSelectorOutput output;
        Combinator combinator = Combinator::None;

        while (true)
        {
            if (selector.steps.size() >= Selector::max_steps)
                throwError(fmt::format("too many compound selectors, the maximum is {}", Selector::max_steps));

            selector.steps.push_back({parseCompound(false), combinator, selector_index});

            bool has_whitespace = skipWhitespace();
            if (pos == end || *pos == ',')
                break;

            if (*pos == ':' && pos + 1 < end && pos[1] == ':')
            {
                parsePseudoElement(output);
                skipWhitespace();
                if (pos != end && *pos != ',')
                    throwError("a pseudo-element must be at the end of a selector");
                break;
            }

            if (*pos == '>' || *pos == '+' || *pos == '~')
            {
                combinator = *pos == '>' ? Combinator::Child : (*pos == '+' ? Combinator::NextSibling : Combinator::SubsequentSibling);
                ++pos;
                skipWhitespace();
            }
            else if (has_whitespace)
            {
                combinator = Combinator::Descendant;
            }
            else
            {
                throwError("unexpected character");
            }
        }

        selector.outputs.push_back(std::move(output));
    }
};

}


Selector::Selector(std::string_view text)
{
    SelectorParser(text).parse(*this);

    for (size_t i = 0; i < steps.size(); ++i)
    {
        UInt64 bit = 1ULL << i;
        const Step & step = steps[i];
        switch (step.combinator)
        {
            case Combinator::None: first_steps |= bit; break;
            case Combinator::Descendant: descendant_steps |= bit; break;
            case Combinator::Child: child_steps |= bit; break;
            case Combinator::NextSibling: next_sibling_steps |= bit; break;
            case Combinator::SubsequentSibling: subsequent_sibling_steps |= bit; break;
        }

        bool is_last = i + 1 == steps.size() || steps[i + 1].selector_index != step.selector_index;
        if (is_last)
            last_steps |= bit;

        if (step.compound.needsAttributes() || (is_last && outputs[step.selector_index].output == Output::Attribute))
            steps_needing_attributes |= bit;
    }
}


/// Classification of HTML elements for the approximate tree construction.
namespace
{

/// Bits of the "kind" of an element.
enum : UInt32
{
    VOID = 1 << 0,                  /// No content and no end tag: `br`, `img`, ...
    RAW_TEXT = 1 << 1,              /// The content is not parsed: `script`, `style`, ...
    ESCAPABLE_RAW_TEXT = 1 << 2,    /// The content is not parsed (but may contain entities): `textarea`, `title`.
    SPECIAL = 1 << 3,               /// End tags of ordinary elements (like `b`) don't close these elements.
    SCOPE = 1 << 4,                 /// End tags of special elements (like `div`) don't close these elements.
    TABLE_SCOPE = 1 << 5,           /// End tags of table elements (like `td`) don't close these elements.
    TABLE_PART = 1 << 6,            /// The end tag of this element uses the table scope.
    CLOSES_P = 1 << 7,              /// The start tag closes an open `p`.
    P = 1 << 8,
    LI = 1 << 9,
    DT_DD = 1 << 10,
    TD_TH = 1 << 11,
    TR = 1 << 12,
    TABLE_SECTION = 1 << 13,        /// `thead`, `tbody`, `tfoot`.
    OPTION = 1 << 14,
    OPTGROUP = 1 << 15,
    LIST = 1 << 16,                 /// `ul`, `ol`, `menu`.
    DL = 1 << 17,
    SELECT = 1 << 18,               /// `select`, `datalist`.
    HEAD = 1 << 19,
    A = 1 << 20,
    BUTTON = 1 << 21,
};

struct TagInfo
{
    UInt32 kind = 0;
    /// The start tag implicitly closes the lowest open element of these kinds (and everything above it in the stack),
    /// unless an element of the boundary kinds is found before.
    UInt32 closes = 0;
    UInt32 boundaries = 0;
};

constexpr UInt64 packName(std::string_view name)
{
    UInt64 res = 0;
    for (size_t i = 0; i < name.size(); ++i)
        res |= static_cast<UInt64>(static_cast<UInt8>(name[i])) << (8 * i);
    return res;
}

TagInfo classifyTag(std::string_view name)
{
    if (name.size() > 10)
        return {};

    if (name.size() > 8)
    {
        String lower = toLowerCopy(name);
        if (lower == "blockquote" || lower == "figcaption")
            return {SPECIAL | CLOSES_P};
        if (lower == "plaintext")
            return {SPECIAL | CLOSES_P | RAW_TEXT};
        return {};
    }

    UInt64 key = 0;
    for (size_t i = 0; i < name.size(); ++i)
        key |= static_cast<UInt64>(static_cast<UInt8>(toLowerASCII(name[i]))) << (8 * i);

    switch (key)
    {
        /// Void elements.
        case packName("area"): case packName("base"): case packName("basefont"): case packName("bgsound"):
        case packName("br"): case packName("col"): case packName("embed"): case packName("frame"):
        case packName("img"): case packName("input"): case packName("keygen"): case packName("link"):
        case packName("meta"): case packName("param"): case packName("source"): case packName("track"):
        case packName("wbr"):
            return {SPECIAL | VOID};
        case packName("hr"):
            return {SPECIAL | VOID | CLOSES_P};

        /// Raw text elements.
        case packName("script"): case packName("style"): case packName("iframe"):
        case packName("noembed"): case packName("noframes"):
            return {SPECIAL | RAW_TEXT};
        case packName("xmp"):
            return {SPECIAL | RAW_TEXT | CLOSES_P};
        case packName("textarea"): case packName("title"):
            return {SPECIAL | ESCAPABLE_RAW_TEXT};

        /// Elements closing an open paragraph.
        case packName("address"): case packName("article"): case packName("aside"): case packName("center"):
        case packName("details"): case packName("dialog"): case packName("dir"): case packName("div"):
        case packName("fieldset"): case packName("figure"): case packName("footer"): case packName("form"):
        case packName("h1"): case packName("h2"): case packName("h3"): case packName("h4"): case packName("h5"):
        case packName("h6"): case packName("header"): case packName("hgroup"): case packName("main"):
        case packName("nav"): case packName("pre"): case packName("listing"): case packName("section"):
        case packName("search"): case packName("summary"):
            return {SPECIAL | CLOSES_P};
        case packName("p"):
            return {SPECIAL | CLOSES_P | P};

        /// Lists.
        case packName("ul"): case packName("ol"): case packName("menu"):
            return {SPECIAL | CLOSES_P | LIST};
        case packName("li"):
            return {SPECIAL | CLOSES_P | LI, LI, LIST | SCOPE};
        case packName("dl"):
            return {SPECIAL | CLOSES_P | DL};
        case packName("dt"): case packName("dd"):
            return {SPECIAL | CLOSES_P | DT_DD, DT_DD, DL | SCOPE};

        /// Tables.
        case packName("table"):
            return {SPECIAL | CLOSES_P | SCOPE | TABLE_SCOPE | TABLE_PART};
        case packName("caption"):
            return {SPECIAL | SCOPE | TABLE_PART};
        case packName("colgroup"):
            return {SPECIAL | TABLE_PART};
        case packName("thead"): case packName("tbody"): case packName("tfoot"):
            return {SPECIAL | TABLE_PART | TABLE_SECTION, TABLE_SECTION | TR | TD_TH, TABLE_SCOPE};
        case packName("tr"):
            return {SPECIAL | TABLE_PART | TR, TR | TD_TH, TABLE_SECTION | TABLE_SCOPE};
        case packName("td"): case packName("th"):
            return {SPECIAL | SCOPE | TABLE_PART | TD_TH, TD_TH, TR | TABLE_SECTION | TABLE_SCOPE};

        /// Forms.
        case packName("select"):
            return {SPECIAL | SELECT};
        case packName("datalist"):
            return {SELECT};
        case packName("option"):
            return {OPTION, OPTION, SELECT | OPTGROUP | SCOPE};
        case packName("optgroup"):
            return {OPTGROUP, OPTION | OPTGROUP, SELECT | SCOPE};
        case packName("button"):
            return {SPECIAL | BUTTON, BUTTON, SCOPE};

        /// Document structure.
        case packName("html"):
            return {SPECIAL | SCOPE | TABLE_SCOPE};
        case packName("head"):
            return {SPECIAL | HEAD};
        case packName("body"):
            return {SPECIAL, HEAD, TABLE_SCOPE};
        case packName("template"):
            return {SPECIAL | SCOPE | TABLE_SCOPE};
        case packName("applet"): case packName("object"): case packName("marquee"):
            return {SPECIAL | SCOPE};
        case packName("frameset"): case packName("noscript"):
            return {SPECIAL};

        /// Links cannot be nested.
        case packName("a"):
            return {A, A, SCOPE};

        default:
            return {};
    }
}

/// HTML elements cannot be nested deeper (the same limit as in Chromium). Deeper elements are considered empty.
constexpr size_t max_depth = 512;

inline bool isTagNameEnd(char c)
{
    return isHTMLWhitespace(c) || c == '/' || c == '>';
}

inline bool startsWith(const char * pos, const char * end, std::string_view prefix)
{
    return static_cast<size_t>(end - pos) >= prefix.size() && 0 == memcmp(pos, prefix.data(), prefix.size());
}

/// Skips to the position after `terminator` (whose last character is '>'), or to the end.
/// The terminator is searched starting from `from`, but may overlap with the characters before it, starting from `min`.
const char * skipAfter(const char * min, const char * from, const char * end, std::string_view terminator)
{
    const char * pos = from;
    while (true)
    {
        pos = find_first_symbols<'>'>(pos, end);
        if (pos >= end)
            return end;
        ++pos;
        if (static_cast<size_t>(pos - min) >= terminator.size()
            && 0 == memcmp(pos - terminator.size(), terminator.data(), terminator.size()))
            return pos;
    }
}

/// Parses the attributes of a start tag, starting after the tag name.
/// Returns the position after the closing '>' or nullptr if the tag is not terminated.
template <bool record>
const char * parseAttributes(const char * pos, const char * end, VectorWithMemoryTracking<Attribute> & attributes, bool & self_closing)
{
    self_closing = false;
    while (true)
    {
        while (pos < end && (isHTMLWhitespace(*pos) || *pos == '/'))
        {
            self_closing = *pos == '/';
            ++pos;
        }

        if (pos >= end)
            return nullptr;

        if (*pos == '>')
            return pos + 1;

        self_closing = false;

        /// An attribute name may start with '='.
        const char * name_begin = pos;
        pos = find_first_symbols<' ', '\t', '\n', '\r', '\f', '/', '>', '='>(pos + 1, end);
        const char * name_end = pos;

        while (pos < end && isHTMLWhitespace(*pos))
            ++pos;

        std::string_view value;
        if (pos < end && *pos == '=')
        {
            ++pos;
            while (pos < end && isHTMLWhitespace(*pos))
                ++pos;
            if (pos >= end)
                return nullptr;

            if (*pos == '"' || *pos == '\'')
            {
                const char * value_begin = pos + 1;
                pos = *pos == '"' ? find_first_symbols<'"'>(value_begin, end) : find_first_symbols<'\''>(value_begin, end);
                if (pos >= end)
                    return nullptr;
                value = std::string_view(value_begin, pos - value_begin);
                ++pos;
            }
            else
            {
                const char * value_begin = pos;
                pos = find_first_symbols<' ', '\t', '\n', '\r', '\f', '>'>(pos, end);
                value = std::string_view(value_begin, pos - value_begin);
            }
        }

        if constexpr (record)
            attributes.push_back({std::string_view(name_begin, name_end - name_begin), value});
    }
}

/// Finds the end tag of a raw text element. Returns the position of its '<' or the end.
const char * findRawTextEnd(const char * pos, const char * end, std::string_view name)
{
    while (true)
    {
        pos = find_first_symbols<'<'>(pos, end);
        if (pos >= end)
            return end;

        const char * name_begin = pos + 2;
        if (name_begin + name.size() <= end
            && pos[1] == '/'
            && equalsCaseInsensitive(std::string_view(name_begin, name.size()), name)
            && (name_begin + name.size() == end || isTagNameEnd(name_begin[name.size()])))
            return pos;

        ++pos;
    }
}

}


bool Matcher::isComplete() const
{
    return first_only && !matches.empty() && matches.front().closed;
}


bool Matcher::closeElement(size_t content_end, size_t end)
{
    const OpenElement & element = stack.back();
    if (element.match_index >= 0)
    {
        Match & match = matches[element.match_index];
        match.content_end = content_end;
        match.end = end;
        match.closed = true;
    }
    stack.pop_back();
    return isComplete();
}


void Matcher::closeImplicitly(UInt32 targets, UInt32 boundaries, size_t pos, bool & stop)
{
    size_t found = 0;
    for (size_t i = stack.size() - 1; i > 0; --i)
    {
        UInt32 kind = stack[i].kind;
        if (kind & targets)
            found = i;
        if (kind & boundaries)
            break;
    }

    if (found)
        while (stack.size() > found)
            stop |= closeElement(pos, pos);
}


UInt64 Matcher::candidateSteps() const
{
    if (first_only && !matches.empty())
        return 0;

    const OpenElement & parent = stack.back();
    return selector.first_steps
        | ((parent.matched_by_ancestors << 1) & selector.descendant_steps)
        | ((parent.matched << 1) & selector.child_steps)
        | ((parent.matched_by_previous_sibling << 1) & selector.next_sibling_steps)
        | ((parent.matched_by_previous_siblings << 1) & selector.subsequent_sibling_steps);
}


bool Matcher::openElement(std::string_view name, UInt32 kind, size_t begin, size_t content_begin, UInt64 candidates)
{
    OpenElement & parent = stack.back();
    ++parent.children;

    UInt64 matched = 0;
    if (candidates)
    {
        ElementInfo element{name, &attributes, parent.children, stack.size() == 1};
        for (UInt64 bits = candidates; bits; bits &= bits - 1)
        {
            size_t step = std::countr_zero(bits);
            if (matchCompound(selector.steps[step].compound, element))
                matched |= 1ULL << step;
        }
    }

    parent.matched_by_previous_sibling = matched;
    parent.matched_by_previous_siblings |= matched;

    ssize_t match_index = -1;
    for (UInt64 bits = matched & selector.last_steps; bits; bits &= bits - 1)
    {
        size_t step = std::countr_zero(bits);
        const auto & output = selector.outputs[selector.steps[step].selector_index];

        Match match;
        match.begin = begin;
        match.content_begin = content_begin;
        match.output = output.output;
        match.raw_text = kind & RAW_TEXT;
        match.escapable_raw_text = kind & ESCAPABLE_RAW_TEXT;

        if (output.output == Output::Attribute)
        {
            ElementInfo element{name, &attributes, 0, false};
            const Attribute * attribute = element.findAttribute(output.attribute);
            if (!attribute)
                continue;   /// Maybe another selector of the list matches.
            match.attribute = attribute->value;
            match.closed = true;
            matches.push_back(match);
        }
        else
        {
            match_index = matches.size();
            matches.push_back(match);
        }
        break;
    }

    OpenElement element;
    element.name = name;
    element.kind = kind;
    element.matched = matched;
    element.matched_by_ancestors = parent.matched_by_ancestors | matched;
    element.match_index = match_index;
    stack.push_back(element);   /// Note: invalidates `parent`.

    return isComplete();
}


const VectorWithMemoryTracking<Match> & Matcher::find(std::string_view document, bool first_only_)
{
    first_only = first_only_;
    matches.clear();
    stack.clear();
    stack.emplace_back();   /// The root of the document.

    const char * const begin = document.data();
    const char * const end = begin + document.size();
    const char * pos = begin;

    auto offset = [begin](const char * ptr) { return static_cast<size_t>(ptr - begin); };

    while (true)
    {
        pos = find_first_symbols<'<'>(pos, end);
        if (pos + 1 >= end)
            break;

        const char * tag_begin = pos;
        char c = pos[1];

        if (isAlphaASCII(c))
        {
            /// Start tag.
            const char * name_begin = pos + 1;
            const char * name_end = find_first_symbols<' ', '\t', '\n', '\r', '\f', '/', '>'>(name_begin, end);
            std::string_view name(name_begin, name_end - name_begin);
            TagInfo info = classifyTag(name);

            bool stop = false;
            if (info.closes)
                closeImplicitly(info.closes, info.boundaries, offset(tag_begin), stop);
            if (info.kind & CLOSES_P)
                closeImplicitly(P, SCOPE | BUTTON, offset(tag_begin), stop);
            if (stop)
                break;

            UInt64 candidates = candidateSteps();
            bool need_attributes = candidates & selector.steps_needing_attributes;

            attributes.clear();
            bool self_closing = false;
            const char * tag_end = need_attributes
                ? parseAttributes<true>(name_end, end, attributes, self_closing)
                : parseAttributes<false>(name_end, end, attributes, self_closing);

            if (!tag_end)
                break;  /// The tag is not terminated: it is ignored, as in HTML.

            pos = tag_end;

            if (openElement(name, info.kind, offset(tag_begin), offset(tag_end), candidates))
                break;

            if ((info.kind & VOID) || self_closing || stack.size() > max_depth)
            {
                if (closeElement(offset(tag_end), offset(tag_end)))
                    break;
            }
            else if (info.kind & (RAW_TEXT | ESCAPABLE_RAW_TEXT))
            {
                const char * content_end = findRawTextEnd(pos, end, name);
                pos = content_end < end ? skipAfter(content_end, content_end, end, ">") : end;
                if (closeElement(offset(content_end), offset(pos)))
                    break;
            }
        }
        else if (c == '/')
        {
            if (pos + 2 < end && isAlphaASCII(pos[2]))
            {
                /// End tag.
                const char * name_begin = pos + 2;
                const char * name_end = find_first_symbols<' ', '\t', '\n', '\r', '\f', '/', '>'>(name_begin, end);
                std::string_view name(name_begin, name_end - name_begin);

                const char * gt = find_first_symbols<'>'>(name_end, end);
                if (gt >= end)
                    break;
                pos = gt + 1;

                UInt32 kind = classifyTag(name).kind;
                UInt32 boundaries = (kind & TABLE_PART) ? TABLE_SCOPE : ((kind & SPECIAL) ? SCOPE : SPECIAL);

                bool stop = false;
                for (size_t i = stack.size() - 1; i > 0; --i)
                {
                    if (equalsCaseInsensitive(stack[i].name, name))
                    {
                        while (stack.size() > i + 1)
                            stop |= closeElement(offset(tag_begin), offset(tag_begin));
                        stop |= closeElement(offset(tag_begin), offset(pos));
                        break;
                    }
                    if (stack[i].kind & boundaries)
                        break;  /// The end tag is ignored.
                }
                if (stop)
                    break;
            }
            else
            {
                /// `</>` or a bogus comment like `</ ...>`.
                pos = skipAfter(pos, pos + 2, end, ">");
            }
        }
        else if (c == '!')
        {
            if (startsWith(pos, end, "<!--"))
                pos = skipAfter(pos + 2, pos + 4, end, "-->");  /// Note: `<!-->` is an empty comment.
            else if (startsWith(pos, end, "<![CDATA["))
                pos = skipAfter(pos + 9, pos + 9, end, "]]>");
            else
                pos = skipAfter(pos, pos + 2, end, ">");    /// DOCTYPE or a bogus comment.
        }
        else if (c == '?')
        {
            pos = skipAfter(pos, pos + 2, end, ">");        /// Processing instruction.
        }
        else
        {
            ++pos;  /// Just a '<' character in text.
        }
    }

    /// Close everything at the end of the document.
    while (stack.size() > 1 && !isComplete())
        closeElement(document.size(), document.size());

    if (first_only && matches.size() > 1)
        matches.resize(1);

    return matches;
}

}
}
