#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Common/Exception.h>
#include <Common/TypePromotion.h>

#include <algorithm>
#include <ostream>
#include <set>
#include <sstream>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_A_COLUMN;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int LOGICAL_ERROR;
}

using IdentifierNameSet = std::set<String>;

class WriteBuffer;


/** Element of the syntax tree (hereinafter - directed acyclic graph with elements of semantics)
  */
class IAST : public std::enable_shared_from_this<IAST>, public TypePromotion<IAST>
{
public:
    ASTs children;

    virtual ~IAST() = default;
    IAST() = default;
    IAST(const IAST &) = default;
    IAST & operator=(const IAST &) = default;

    /** Get the canonical name of the column if the element is a column */
    String getColumnName() const;
    virtual void appendColumnName(WriteBuffer &) const
    {
        throw Exception("Trying to get name of not a column: " + getID(), ErrorCodes::NOT_A_COLUMN);
    }

    /** Get the alias, if any, or the canonical name of the column, if it is not. */
    virtual String getAliasOrColumnName() const { return getColumnName(); }

    /** Get the alias, if any, or an empty string if it does not exist, or if the element does not support aliases. */
    virtual String tryGetAlias() const { return String(); }

    /** Set the alias. */
    virtual void setAlias(const String & /*to*/)
    {
        throw Exception("Can't set alias of " + getColumnName(), ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
    }

    /** Get the text that identifies this element. */
    virtual String getID(char delimiter = '_') const = 0;

    ASTPtr ptr() { return shared_from_this(); }

    /** Get a deep copy of the tree. Cloned object must have the same range. */
    virtual ASTPtr clone() const = 0;

    /** Get hash code, identifying this element and its subtree.
      */
    using Hash = std::pair<UInt64, UInt64>;
    Hash getTreeHash() const;
    void updateTreeHash(SipHash & hash_state) const;
    virtual void updateTreeHashImpl(SipHash & hash_state) const;

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

    /** Get total number of tree elements
     */
    size_t size() const;

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

    template <typename T>
    void set(T * & field, const ASTPtr & child)
    {
        if (!child)
            return;

        T * casted = dynamic_cast<T *>(child.get());
        if (!casted)
            throw Exception("Could not cast AST subtree", ErrorCodes::LOGICAL_ERROR);

        children.push_back(child);
        field = casted;
    }

    template <typename T>
    void replace(T * & field, const ASTPtr & child)
    {
        if (!child)
            throw Exception("Trying to replace AST subtree with nullptr", ErrorCodes::LOGICAL_ERROR);

        T * casted = dynamic_cast<T *>(child.get());
        if (!casted)
            throw Exception("Could not cast AST subtree", ErrorCodes::LOGICAL_ERROR);

        for (ASTPtr & current_child : children)
        {
            if (current_child.get() == field)
            {
                current_child = child;
                field = casted;
                return;
            }
        }

        throw Exception("AST subtree not found in children", ErrorCodes::LOGICAL_ERROR);
    }

    template <typename T>
    void setOrReplace(T * & field, const ASTPtr & child)
    {
        if (field)
            replace(field, child);
        else
            set(field, child);
    }

    /// Convert to a string.

    /// Format settings.
    struct FormatSettings
    {
        std::ostream & ostr;
        bool hilite = false;
        bool one_line;
        bool always_quote_identifiers = false;
        IdentifierQuotingStyle identifier_quoting_style = IdentifierQuotingStyle::Backticks;

        char nl_or_ws;

        FormatSettings(std::ostream & ostr_, bool one_line_)
            : ostr(ostr_), one_line(one_line_)
        {
            nl_or_ws = one_line ? ' ' : '\n';
        }

        FormatSettings(std::ostream & ostr_, const FormatSettings & other)
            : ostr(ostr_), hilite(other.hilite), one_line(other.one_line),
            always_quote_identifiers(other.always_quote_identifiers), identifier_quoting_style(other.identifier_quoting_style)
        {
            nl_or_ws = one_line ? ' ' : '\n';
        }

        void writeIdentifier(const String & name) const;
    };

    /// State. For example, a set of nodes can be remembered, which we already walk through.
    struct FormatState
    {
        /** The SELECT query in which the alias was found; identifier of a node with such an alias.
          * It is necessary that when the node has met again, output only the alias.
          */
        std::set<std::tuple<
            const IAST * /* SELECT query node */,
            std::string /* alias */,
            Hash /* printed content */>> printed_asts_with_alias;
    };

    /// The state that is copied when each node is formatted. For example, nesting level.
    struct FormatStateStacked
    {
        UInt8 indent = 0;
        bool need_parens = false;
        bool expression_list_always_start_on_new_line = false;  /// Line feed and indent before expression list even if it's of single element.
        const IAST * current_select = nullptr;
    };

    void format(const FormatSettings & settings) const
    {
        FormatState state;
        formatImpl(settings, state, FormatStateStacked());
    }

    virtual void formatImpl(const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const
    {
        throw Exception("Unknown element in AST: " + getID(), ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
    }

    // A simple way to add some user-readable context to an error message.
    std::string formatForErrorMessage() const;
    template <typename AstArray>
    static std::string formatForErrorMessage(const AstArray & array);

    void cloneChildren();

public:
    /// For syntax highlighting.
    static const char * hilite_keyword;
    static const char * hilite_identifier;
    static const char * hilite_function;
    static const char * hilite_operator;
    static const char * hilite_alias;
    static const char * hilite_substitution;
    static const char * hilite_none;

private:
    size_t checkDepthImpl(size_t max_depth, size_t level) const;
};

template <typename AstArray>
std::string IAST::formatForErrorMessage(const AstArray & array)
{
    std::stringstream ss;
    for (size_t i = 0; i < array.size(); ++i)
    {
        if (i > 0)
        {
            ss << ", ";
        }
        array[i]->format(IAST::FormatSettings(ss, true /* one line */));
    }
    return ss.str();
}

}
