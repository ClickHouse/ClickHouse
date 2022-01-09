#pragma once

#include <memory>
#include <vector>

#include <Core/Defines.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/TokenIterator.h>
#include <base/types.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int LOGICAL_ERROR;
}


/** Collects variants, how parser could proceed further at rightmost position.
  */
struct Expected
{
    const char * max_parsed_pos = nullptr;
    std::vector<const char *> variants;

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

        if ((current_pos == max_parsed_pos) && (find(variants.begin(), variants.end(), description) == variants.end()))
            variants.push_back(description);
    }

    ALWAYS_INLINE void add(TokenIterator it, const char * description)
    {
        add(it->begin, description);
    }
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

        Pos(Tokens & tokens_, uint32_t max_depth_) : TokenIterator(tokens_), max_depth(max_depth_)
        {
        }

        ALWAYS_INLINE void increaseDepth()
        {
            ++depth;
            if (unlikely(max_depth > 0 && depth > max_depth))
                throw Exception(
                    "Maximum parse depth (" + std::to_string(max_depth) + ") exceeded. Consider rising max_parser_depth parameter.",
                    ErrorCodes::TOO_DEEP_RECURSION);
        }

        ALWAYS_INLINE void decreaseDepth()
        {
            if (unlikely(depth == 0))
                throw Exception("Logical error in parser: incorrect calculation of parse depth", ErrorCodes::LOGICAL_ERROR);
            --depth;
        }
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

    bool ignore(Pos & pos, Expected & expected)  // -V1071
    {
        ASTPtr ignore_node;
        return parse(pos, ignore_node, expected);
    }

    bool ignore(Pos & pos)  // -V1071
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
        else
            return true;
    }

    /** The same, but doesn't move the position even if parsing was successful.
     */
    bool checkWithoutMoving(Pos pos, Expected & expected)
    {
        ASTPtr node;
        return parse(pos, node, expected);
    }

    virtual ~IParser() = default;
};

using ParserPtr = std::unique_ptr<IParser>;

}
