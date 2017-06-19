#pragma once

#include <set>
#include <memory>
#include <ostream>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/StringRange.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_A_COLUMN;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int UNKNOWN_ELEMENT_IN_AST;
}

using IdentifierNameSet = std::set<String>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

class WriteBuffer;


/** Element of the syntax tree (hereinafter - directed acyclic graph with elements of semantics)
  */
class IAST
{
public:
    ASTs children;
    StringRange range;

    /** A string with a full query.
      * This pointer does not allow it to be deleted while the range refers to it.
      */
    StringPtr query_string;

    IAST() = default;
    IAST(const StringRange range_) : range(range_) {}
    virtual ~IAST() = default;

    /** Get the canonical name of the column if the element is a column */
    virtual String getColumnName() const { throw Exception("Trying to get name of not a column: " + getID(), ErrorCodes::NOT_A_COLUMN); }

    /** Get the alias, if any, or the canonical name of the column, if it is not. */
    virtual String getAliasOrColumnName() const { return getColumnName(); }

    /** Get the alias, if any, or an empty string if it does not exist, or if the element does not support aliases. */
    virtual String tryGetAlias() const { return String(); }

    /** Set the alias. */
    virtual void setAlias(const String & to)
    {
        throw Exception("Can't set alias of " + getColumnName(), ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
    }

    /** Get the text that identifies this element. */
    virtual String getID() const = 0;

    /** Get a deep copy of the tree. */
    virtual ASTPtr clone() const = 0;

    /** Get text, describing and identifying this element and its subtree.
      * Usually it consist of element's id and getTreeID of all children.
      */
    String getTreeID() const;
    void getTreeIDImpl(WriteBuffer & out) const;

    /** Get hash code, identifying this element and its subtree.
      */
    using Hash = std::pair<UInt64, UInt64>;
    Hash getTreeHash() const;
    void getTreeHashImpl(SipHash & hash_state) const;

    void dumpTree(std::ostream & ostr, size_t indent = 0) const
    {
        String indent_str(indent, '-');
        ostr << indent_str << getID() << ", " << this << std::endl;
        for (const auto & child : children)
            child->dumpTree(ostr, indent + 1);
    }

    /** Check the depth of the tree.
      * If max_depth is specified and the depth is greater - throw an exception.
      * Returns the depth of the tree.
      */
    size_t checkDepth(size_t max_depth) const
    {
        return checkDepthImpl(max_depth, 0);
    }

    /** Same for the total number of tree elements.
      */
    size_t checkSize(size_t max_size) const;

    /** Get `set` from the names of the identifiers
     */
    virtual void collectIdentifierNames(IdentifierNameSet & set) const
    {
        for (const auto & child : children)
            child->collectIdentifierNames(set);
    }


    /// Convert to a string.

    /// Format settings.
    struct FormatSettings
    {
        std::ostream & ostr;
        bool hilite;
        bool one_line;

        char nl_or_ws;

        FormatSettings(std::ostream & ostr_, bool hilite_, bool one_line_)
            : ostr(ostr_), hilite(hilite_), one_line(one_line_)
        {
            nl_or_ws = one_line ? ' ' : '\n';
        }
    };

    /// State. For example, a set of nodes can be remembered, which we already walk through.
    struct FormatState
    {
        /** The SELECT query in which the alias was found; identifier of a node with such an alias.
          * It is necessary that when the node has met again, output only the alias.
          */
        std::set<std::pair<const IAST *, std::string>> printed_asts_with_alias;
    };

    /// The state that is copied when each node is formatted. For example, nesting level.
    struct FormatStateStacked
    {
        UInt8 indent = 0;
        bool need_parens = false;
        const IAST * current_select = nullptr;
    };

    void format(const FormatSettings & settings) const
    {
        FormatState state;
        formatImpl(settings, state, FormatStateStacked());
    }

    virtual void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
    {
        throw Exception("Unknown element in AST: " + getID()
            + ((range.first && (range.second > range.first))
                ? " '" + std::string(range.first, range.second - range.first) + "'"
                : ""),
            ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
    }

    void writeAlias(const String & name, std::ostream & s, bool hilite) const;

public:
    /// For syntax highlighting.
    static const char * hilite_keyword;
    static const char * hilite_identifier;
    static const char * hilite_function;
    static const char * hilite_operator;
    static const char * hilite_alias;
    static const char * hilite_none;

private:
    size_t checkDepthImpl(size_t max_depth, size_t level) const;
};


/// Surrounds an identifier by back quotes if it is necessary.
String backQuoteIfNeed(const String & x);


}
