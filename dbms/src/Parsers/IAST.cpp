#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>
#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_BIG_AST;
    extern const int TOO_DEEP_AST;
}


const char * IAST::hilite_keyword    = "\033[1m";
const char * IAST::hilite_identifier = "\033[0;36m";
const char * IAST::hilite_function   = "\033[0;33m";
const char * IAST::hilite_operator   = "\033[1;33m";
const char * IAST::hilite_alias      = "\033[0;32m";
const char * IAST::hilite_none       = "\033[0m";


/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(const String & x)
{
    String res(x.size(), '\0');
    {
        WriteBufferFromString wb(res);
        writeProbablyBackQuotedString(x, wb);
    }
    return res;
}


void IAST::writeAlias(const String & name, std::ostream & s, bool hilite) const
{
    s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_alias : "");

    WriteBufferFromOStream wb(s, 32);
    writeProbablyBackQuotedString(name, wb);
    wb.next();

    s << (hilite ? hilite_none : "");
}


size_t IAST::checkSize(size_t max_size) const
{
    size_t res = 1;
    for (const auto & child : children)
        res += child->checkSize(max_size);

    if (res > max_size)
        throw Exception("AST is too big. Maximum: " + toString(max_size), ErrorCodes::TOO_BIG_AST);

    return res;
}


IAST::Hash IAST::getTreeHash() const
{
    SipHash hash_state;
    getTreeHashImpl(hash_state);
    IAST::Hash res;
    hash_state.get128(res.first, res.second);
    return res;
}


void IAST::getTreeHashImpl(SipHash & hash_state) const
{
    auto id = getID();
    hash_state.update(id.data(), id.size());

    size_t num_children = children.size();
    hash_state.update(reinterpret_cast<const char *>(&num_children), sizeof(num_children));

    for (const auto & child : children)
        child->getTreeHashImpl(hash_state);
}


size_t IAST::checkDepthImpl(size_t max_depth, size_t level) const
{
    size_t res = level + 1;
    for (const auto & child : children)
    {
        if (level >= max_depth)
            throw Exception("AST is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_AST);
        res = std::max(res, child->checkDepthImpl(max_depth, level + 1));
    }

    return res;
}

}
