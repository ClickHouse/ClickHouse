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
    extern const int BAD_ARGUMENTS;
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
    hash_state.update(children.size());
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


void IAST::cloneChildren()
{
    for (auto & child : children)
        child = child->clone();
}


String IAST::getColumnName() const
{
    WriteBufferFromOwnString write_buffer;
    appendColumnName(write_buffer);
    return write_buffer.str();
}


void IAST::FormatSettings::writeIdentifier(const String & name) const
{
    WriteBufferFromOStream out(ostr, 32);

    switch (identifier_quoting_style)
    {
        case IdentifierQuotingStyle::None:
        {
            if (always_quote_identifiers)
                throw Exception("Incompatible arguments: always_quote_identifiers = true && identifier_quoting_style == IdentifierQuotingStyle::None",
                    ErrorCodes::BAD_ARGUMENTS);
            writeString(name, out);
            break;
        }
        case IdentifierQuotingStyle::Backticks:
        {
            if (always_quote_identifiers)
                writeBackQuotedString(name, out);
            else
                writeProbablyBackQuotedString(name, out);
            break;
        }
        case IdentifierQuotingStyle::DoubleQuotes:
        {
            if (always_quote_identifiers)
                writeDoubleQuotedString(name, out);
            else
                writeProbablyDoubleQuotedString(name, out);
            break;
        }
    }

    out.next();
}

}
