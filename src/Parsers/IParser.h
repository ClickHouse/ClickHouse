#pragma once

#include <absl/container/inlined_vector.h>
#include <set>
#include <algorithm>
#include <memory>

#include <Core/Defines.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/TokenIterator.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int LOGICAL_ERROR;
}

enum class Highlight : uint8_t
{
    none = 0,
    keyword,
    identifier,
    function,
    alias,
    substitution,
    number,
    string,
};

struct HighlightedRange
{
    const char * begin;
    const char * end;
    Highlight highlight;

    auto operator<=>(const HighlightedRange & other) const
    {
        return begin <=> other.begin;
    }
};


/** Collects variants, how parser could proceed further at rightmost position.
  * Also collects a mapping of parsed ranges for highlighting,
  * which is accumulated through the parsing.
  */
struct Expected
{
    absl::InlinedVector<const char *, 7> variants;
    const char * max_parsed_pos = nullptr;

    bool enable_highlighting = false;
    std::set<HighlightedRange> highlights;

    /// 'description' should be statically allocated string.
    ALWAYS_INLINE void add(const char * current_pos, const char * description)
    {
        if (!max_parsed_pos || current_pos > max_parsed_pos)
        {
            variants.clear();
            max_parsed_pos = current_pos;
            variants.push_back(description);
            return;
        }

        if ((current_pos == max_parsed_pos) && (std::find(variants.begin(), variants.end(), description) == variants.end()))
            variants.push_back(description);
    }

    ALWAYS_INLINE void add(TokenIterator it, const char * description)
    {
        add(it->begin, description);
    }

    void highlight(HighlightedRange range);
};


/** Interface for parser classes
  */
class IParser
{
public:
    /// Token iterator augmented with depth information. This allows to control recursion depth.
    struct Pos : TokenIterator
    {
        uint32_t depth = 0;
        uint32_t max_depth = 0;

        uint32_t backtracks = 0;
        uint32_t max_backtracks = 0;

        Pos(Tokens & tokens_, uint32_t max_depth_, uint32_t max_backtracks_)
            : TokenIterator(tokens_), max_depth(max_depth_), max_backtracks(max_backtracks_)
        {
        }

        Pos(TokenIterator token_iterator_, uint32_t max_depth_, uint32_t max_backtracks_)
            : TokenIterator(token_iterator_), max_depth(max_depth_), max_backtracks(max_backtracks_)
        {
        }

        ALWAYS_INLINE void increaseDepth()
        {
            ++depth;
            if (unlikely(max_depth > 0 && depth > max_depth))
                throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Maximum parse depth ({}) exceeded. "
                    "Consider rising max_parser_depth parameter.", max_depth);

            /** Sometimes the maximum parser depth can be set to a high value by the user,
              * but we still want to avoid stack overflow.
              * For this purpose, we can use the checkStackSize function, but it is too heavy.
              * The solution is to check not too frequently.
              * The frequency is arbitrary, but not too large, not too small,
              * and a power of two to simplify the division.
              */
#if defined(USE_MUSL) || defined(SANITIZER) || !defined(NDEBUG)
            static constexpr uint32_t check_frequency = 128;
#else
            static constexpr uint32_t check_frequency = 8192;
#endif
            if (depth % check_frequency == 0)
                checkStackSize();
        }

        ALWAYS_INLINE void decreaseDepth()
        {
            if (unlikely(depth == 0))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error in parser: incorrect calculation of parse depth");
            --depth;
        }

        Pos(const Pos & rhs) = default;

        Pos & operator=(const Pos & rhs);
    };

    /** Get the text of this parser parses. */
    virtual const char * getName() const = 0;

    /** Parse piece of text from position `pos`, but not beyond end of line (`end` - position after end of line),
      * move pointer `pos` to the maximum position to which it was possible to parse,
      * in case of success return `true` and the result in `node` if it is needed, otherwise false,
      * in `expected` write what was expected in the maximum position,
      *  to which it was possible to parse if parsing was unsuccessful,
      *  or what this parser parse if parsing was successful.
      * The string to which the [begin, end) range is included may be not 0-terminated.
      */
    virtual bool parse(Pos & pos, ASTPtr & node, Expected & expected) = 0;

    bool ignore(Pos & pos, Expected & expected)
    {
        ASTPtr ignore_node;
        return parse(pos, ignore_node, expected);
    }

    bool ignore(Pos & pos)
    {
        Expected expected;
        return ignore(pos, expected);
    }

    /** The same, but do not move the position and do not write the result to node.
      */
    bool check(Pos & pos, Expected & expected)
    {
        Pos begin = pos;
        ASTPtr node;
        if (!parse(pos, node, expected))
        {
            pos = begin;
            return false;
        }
        return true;
    }

    /** The same, but doesn't move the position even if parsing was successful.
     */
    bool checkWithoutMoving(Pos pos, Expected & expected)
    {
        ASTPtr node;
        return parse(pos, node, expected);
    }

    /** If the parsed fragment should be highlighted in the query editor,
      * which type of highlighting to use?
      */
    virtual Highlight highlight() const
    {
        return Highlight::none;
    }

    virtual ~IParser() = default;
};

using ParserPtr = std::unique_ptr<IParser>;

}
