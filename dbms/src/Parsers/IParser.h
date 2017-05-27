#pragma once

#include <list>
#include <memory>

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Parsers/IAST.h>


namespace DB
{

using Expected = const char *;


/** Interface for parser classes
  */
class IParser
{
public:
    using Pos = const char *;

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
    virtual bool parse(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) = 0;

    bool ignore(Pos & pos, Pos end, Pos & max_parsed_pos, Expected & expected)
    {
        ASTPtr ignore_node;
        return parse(pos, end, ignore_node, max_parsed_pos, expected);
    }

    bool ignore(Pos & pos, Pos end)
    {
        Pos max_parsed_pos = pos;
        Expected expected;
        return ignore(pos, end, max_parsed_pos, expected);
    }

    /** The same, but do not move the position and do not write the result to node.
      */
    bool check(Pos & pos, Pos end, Pos & max_parsed_pos, Expected & expected)
    {
        Pos begin = pos;
        ASTPtr node;
        if (!parse(pos, end, node, max_parsed_pos, expected))
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
