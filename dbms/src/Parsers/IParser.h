#pragma once

#include <set>
#include <memory>

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Parsers/TokenIterator.h>


namespace DB
{

/** Collects variants, how parser could proceed further at rightmost position.
  */
struct Expected
{
    const char * max_parsed_pos = nullptr;
    std::set<const char *> variants;

    /// 'description' should be statically allocated string.
    void add(const char * current_pos, const char * description)
    {
        if (!max_parsed_pos || current_pos > max_parsed_pos)
        {
            variants.clear();
            max_parsed_pos = current_pos;
        }

        if (!max_parsed_pos || current_pos >= max_parsed_pos)
            variants.insert(description);
    }

    void add(TokenIterator it, const char * description)
    {
        add(it->begin, description);
    }
};


/** Interface for parser classes
  */
class IParser
{
public:
    using Pos = TokenIterator;

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
        else
            return true;
    }

    virtual ~IParser() {}
};

using ParserPtr = std::unique_ptr<IParser>;

}
