#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Common/Exception.h>
#include <Common/TypePromotion.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <set>
#include <list>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using IdentifierNameSet = std::set<String>;

class WriteBuffer;
using Strings = std::vector<String>;

/** Element of the syntax tree (hereinafter - directed acyclic graph with elements of semantics)
  */
class IAST : public std::enable_shared_from_this<IAST>, public TypePromotion<IAST>
{
public:
    ASTs children;

    virtual ~IAST();
    IAST() = default;
    IAST(const IAST &) = default;
    IAST & operator=(const IAST &) = default;

    /** Get the canonical name of the column if the element is a column */
    String getColumnName() const;

    /** Same as the above but ensure no alias names are used. This is for index analysis */
    String getColumnNameWithoutAlias() const;

    virtual void appendColumnName(WriteBuffer &) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get name of not a column: {}", getID());
    }

    virtual void appendColumnNameWithoutAlias(WriteBuffer &) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get name of not a column: {}", getID());
    }

    /** Get the alias, if any, or the canonical name of the column, if it is not. */
    virtual String getAliasOrColumnName() const { return getColumnName(); }

    /** Get the alias, if any, or an empty string if it does not exist, or if the element does not support aliases. */
    virtual String tryGetAlias() const { return String(); }

    /** Set the alias. */
    virtual void setAlias(const String & /*to*/)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't set alias of {}", getColumnName());
    }

    /** Get the text that identifies this element. */
    virtual String getID(char delimiter = '_') const = 0; /// NOLINT

    ASTPtr ptr() { return shared_from_this(); }

    /** Get a deep copy of the tree. Cloned object must have the same range. */
    virtual ASTPtr clone() const = 0;

    /** Get hash code, identifying this element and its subtree.
      */
    using Hash = std::pair<UInt64, UInt64>;
    Hash getTreeHash() const;
    void updateTreeHash(SipHash & hash_state) const;
    virtual void updateTreeHashImpl(SipHash & hash_state) const;

    void dumpTree(WriteBuffer & ostr, size_t indent = 0) const;
    std::string dumpTree(size_t indent = 0) const;

    /** Check the depth of the tree.
      * If max_depth is specified and the depth is greater - throw an exception.
      * Returns the depth of the tree.
      */
    size_t checkDepth(size_t max_depth) const
    {
        return checkDepthImpl(max_depth);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not cast AST subtree");

        children.push_back(child);
        field = casted;
    }

    template <typename T>
    void replace(T * & field, const ASTPtr & child)
    {
        if (!child)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to replace AST subtree with nullptr");

        T * casted = dynamic_cast<T *>(child.get());
        if (!casted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not cast AST subtree");

        for (ASTPtr & current_child : children)
        {
            if (current_child.get() == field)
            {
                current_child = child;
                field = casted;
                return;
            }
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "AST subtree not found in children");
    }

    template <typename T>
    void setOrReplace(T * & field, const ASTPtr & child)
    {
        if (field)
            replace(field, child);
        else
            set(field, child);
    }

    template <typename T>
    void reset(T * & field)
    {
        if (field == nullptr)
            return;

        const auto child = std::find_if(children.begin(), children.end(), [field](const auto & p)
        {
           return p.get() == field;
        });

        if (child == children.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AST subtree not found in children");

        children.erase(child);
        field = nullptr;
    }

    /// After changing one of `children` elements, update the corresponding member pointer if needed.
    void updatePointerToChild(void * old_ptr, void * new_ptr)
    {
        forEachPointerToChild([old_ptr, new_ptr](void ** ptr) mutable
        {
            if (*ptr == old_ptr)
                *ptr = new_ptr;
        });
    }

    /// Convert to a string.

    /// Format settings.
    struct FormatSettings
    {
        bool hilite = false;
        bool one_line = true;
        bool always_quote_identifiers = false;
        IdentifierQuotingStyle identifier_quoting_style = IdentifierQuotingStyle::Backticks;
        bool show_secrets = true; /// Show secret parts of the AST (e.g. passwords, encryption keys).

        FormatSettings copyWithAlwaysQuoteIdentifiers() const;
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
        bool expression_list_prepend_whitespace = false; /// Prepend whitespace (if it is required)
        bool surround_each_list_element_with_parens = false;
        const IAST * current_select = nullptr;
    };

    class FormattingBuffer
    {
    public:
        WriteBuffer & ostr;

        static const char * hilite_keyword;
        static const char * hilite_identifier;
        static const char * hilite_function;
        static const char * hilite_operator;
        static const char * hilite_alias;
        static const char * hilite_substitution;
        static const char * hilite_none;
        static const char * hilite_metacharacter;

    private:
        const FormatSettings & settings;
        FormatState & state;
        FormatStateStacked stacked_state;


        /**
         * To be used in RAII style, just like std::lock_guard with std::mutex.
         */
        class Hiliter
        {
            DB::WriteBuffer & ostr;
            bool hilite;
        public:
            Hiliter(DB::WriteBuffer & ostr_, bool hilite_, const char * hilite_type);
            ~Hiliter();

            Hiliter(const Hiliter & other) = delete;
            Hiliter(Hiliter && other) = delete;
            Hiliter & operator=(const Hiliter & other) = delete;
            Hiliter & operator=(Hiliter && other) = delete;
        };
        Hiliter createHiliter(const char * hilite_type) const;
        void writePossiblyHilited(std::string_view str, const char * hilite_type) const;
        void writeIdentifierOrAlias(const String & name, bool hilite_as_identifier) const;

        FormattingBuffer(WriteBuffer & ostr_, const FormatSettings & settings_, FormatState & state_, FormatStateStacked stacked_state_) :
            ostr(ostr_), settings(settings_), state(state_), stacked_state(stacked_state_) {}
    public:
        FormattingBuffer(WriteBuffer & ostr_, const FormatSettings & settings_, FormatState & state_) :
            ostr(ostr_), settings(settings_), state(state_) {}

        FormattingBuffer copy() const;

        const FormatSettings & getSettings() const;

        FormattingBuffer copyWithNewSettings(const FormatSettings & settings_) const;

        FormattingBuffer & setIndent(UInt8 indent);

        FormattingBuffer & increaseIndent(int extra_indent = 1);

        FormattingBuffer & setNeedParens(bool value = true);

        FormattingBuffer & setExpressionListAlwaysStartsOnNewLine(bool value = true);

        FormattingBuffer & setExpressionListPrependWhitespace(bool value = true);

        FormattingBuffer & setSurroundEachListElementWithParens(bool value = true);

        FormattingBuffer & setCurrentSelect(const IAST * select);

        bool insertAlias(std::string alias, Hash printed_content) const;

        void writeKeyword(std::string_view str) const;
        void writeFunction(std::string_view str) const;
        void writeOperator(std::string_view str) const;
        void writeSubstitution(std::string_view str) const;

        /** A special hack. If it's [I]LIKE or NOT [I]LIKE expression and the right hand side is a string literal,
          *  we will highlight unescaped metacharacters % and _ in string literal for convenience.
          * Motivation: most people are unaware that _ is a metacharacter and forgot to properly escape it with two backslashes.
          * With highlighting we make it clearly obvious.
          *
          * Another case is regexp match. Suppose the user types match(URL, 'www.clickhouse.com'). It often means that the user is unaware
          *  that . is a metacharacter.
          */
        void writeStringLiteralWithMetacharacters(const String & str, const char * metacharacters) const;

        void writeIdentifier(const String & name) const;
        void writeAlias(const String & name) const;
        void writeProbablyBackQuotedIdentifier(const String & name) const;

        void nlOrWs() const;  // Write newline or whitespace.
        void nlOrNothing() const;

        void writeSecret(const String & secret = "") const;
        void writeIndent(int extra_indent = 0) const;

        bool isOneLine() const;
        bool shouldShowSecrets() const;

        bool needParens() const;
        bool getExpressionListAlwaysStartsOnNewLine() const;
        bool getExpressionListPrependWhitespace() const;
        bool getSurroundEachListElementWithParens() const;
    };

    // With new a blank internal state.
    void format(WriteBuffer & ostr, const FormatSettings & settings) const
    {
        FormatState state;
        formatImpl(FormattingBuffer(ostr, settings, state));
    }

    // Keeping the state.
    virtual void formatImpl(FormattingBuffer /*out*/) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown element in AST: {}", getID());
    }

    // A simple way to add some user-readable context to an error message.
    String formatWithSecretsHidden(size_t max_length = 0, bool one_line = true) const;
    String formatForLogging(size_t max_length = 0) const { return formatWithSecretsHidden(max_length, true); }
    String formatForErrorMessage() const { return formatWithSecretsHidden(0, true); }

    /// If an AST has secret parts then formatForLogging() will replace them with the placeholder '[HIDDEN]'.
    virtual bool hasSecretParts() const { return childrenHaveSecretParts(); }

    void cloneChildren();

    enum class QueryKind : uint8_t
    {
        None = 0,
        Select,
        Insert,
        Delete,
        Create,
        Drop,
        Undrop,
        Rename,
        Optimize,
        Check,
        Alter,
        Grant,
        Revoke,
        System,
        Set,
        Use,
        Show,
        Exists,
        Describe,
        Explain,
        Backup,
        Restore,
        KillQuery,
        ExternalDDL,
        Begin,
        Commit,
        Rollback,
        SetTransactionSnapshot,
    };
    /// Return QueryKind of this AST query.
    virtual QueryKind getQueryKind() const { return QueryKind::None; }

protected:
    bool childrenHaveSecretParts() const;

    /// Some AST classes have naked pointers to children elements as members.
    /// This method allows to iterate over them.
    virtual void forEachPointerToChild(std::function<void(void**)>) {}

private:
    size_t checkDepthImpl(size_t max_depth) const;

    /** Forward linked list of ASTPtr to delete.
      * Used in IAST destructor to avoid possible stack overflow.
      */
    ASTPtr next_to_delete = nullptr;
    ASTPtr * next_to_delete_list_head = nullptr;
};

}
