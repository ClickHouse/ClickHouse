#pragma once

#include <Parsers/IASTHash.h>
#include <Parsers/IAST_fwd.h>

#include <set>
#include <tuple>

namespace DB
{
/// State. For example, a set of nodes can be remembered, which we already walk through.
struct IASTFormatState
{
    /** The SELECT query in which the alias was found; identifier of a node with such an alias.
      * It is necessary that when the node has met again, output only the alias.
      * This is only needed for the old analyzer. Remove it after removing the old analyzer.
      */
    std::set<std::tuple<const IAST * /* SELECT query node */, std::string /* alias */, IASTHash /* printed content */>>
        printed_asts_with_alias;
};
}
