#include <Parsers/IAST.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Poco/String.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>

#include <algorithm>

namespace DB
{

/// Verify that we did not increase the size of IAST by accident.
static_assert(sizeof(IAST) <= 32);

void intrusive_ptr_add_ref(const IAST * p) noexcept
{
    p->ref_counter.fetch_add(1, std::memory_order_relaxed);
}

void intrusive_ptr_release(const IAST * p) noexcept
{
    if (p->ref_counter.fetch_sub(1, std::memory_order_acq_rel) == 1)
    {
        struct LinkedList
        {
            ASTs children;
            LinkedList * next = nullptr;

            static LinkedList * create(IAST * ptr)
            {
                if (ptr->children.empty())
                {
                    delete ptr;
                    return nullptr;
                }

                ASTs children;
                children.swap(ptr->children);
                ptr->~IAST();
                LinkedList * elem = new (dynamic_cast<void *>(ptr)) LinkedList;
                elem->children.swap(children);
                return elem;
            }
        };

        static_assert(sizeof(LinkedList) <= sizeof(IAST));

        const IAST * const_ptr = p;
        IAST * ptr = const_cast<IAST *>(const_ptr);

        LinkedList * list_head = LinkedList::create(ptr);

        while (list_head)
        {
            ASTs children;
            children.swap(list_head->children);
            {
                LinkedList * next = list_head->next;
                list_head->~LinkedList();
                operator delete(list_head);
                list_head = next;
            }

            for (auto & child : children)
            {
                if (child == nullptr || child->use_count() != 1)
                    continue;

                ptr = child.detach();
                chassert(ptr->ref_counter.fetch_sub(1, std::memory_order_acq_rel) == 1);

                LinkedList * elem = LinkedList::create(ptr);
                if (elem)
                {
                    elem->next = list_head;
                    list_head = elem;
                }
            }
        }
    }
}

namespace ErrorCodes
{
    extern const int TOO_BIG_AST;
    extern const int TOO_DEEP_AST;
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

IAST::IAST(const IAST & other)
    : TypePromotion<IAST>()
    , children(other.children)
    , ref_counter(0)
    , flags_storage(other.flags_storage)
{
}

IAST & IAST::operator=(const IAST & other)
{
    if (this == &other)
        return *this;
    children = other.children;
    flags_storage = other.flags_storage;
    return *this;
}

IAST::~IAST() = default;

size_t IAST::size() const
{
    size_t res = 1;
    for (const auto & child : children)
        res += child->size();

    return res;
}

size_t IAST::checkSize(size_t max_size) const
{
    size_t res = 1;
    for (const auto & child : children)
        res += child->checkSize(max_size);

    if (res > max_size)
        throw Exception(ErrorCodes::TOO_BIG_AST, "AST is too big. Maximum: {}", max_size);

    return res;
}


IASTHash IAST::getTreeHash(bool ignore_aliases) const
{
    SipHash hash_state;
    updateTreeHash(hash_state, ignore_aliases);
    return getSipHash128AsPair(hash_state);
}


void IAST::updateTreeHash(SipHash & hash_state, bool ignore_aliases) const
{
    updateTreeHashImpl(hash_state, ignore_aliases);
    hash_state.update(children.size());
    for (const auto & child : children)
        child->updateTreeHash(hash_state, ignore_aliases);
}


void IAST::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    auto id = getID();
    hash_state.update(id.data(), id.size());
}


size_t IAST::checkDepthImpl(size_t max_depth) const
{
    std::vector<std::pair<ASTPtr, size_t>> stack;
    stack.reserve(children.size());

    for (const auto & i: children)
        stack.push_back({i, 1});

    size_t res = 0;

    while (!stack.empty())
    {
        auto top = stack.back();
        stack.pop_back();

        if (top.second >= max_depth)
            throw Exception(ErrorCodes::TOO_DEEP_AST, "AST is too deep. Maximum: {}", max_depth);

        res = std::max(res, top.second);

        for (const auto & i: top.first->children)
            stack.push_back({i, top.second + 1});
    }

    return res;
}

String IAST::formatWithPossiblyHidingSensitiveData(
    size_t max_length,
    bool one_line,
    bool show_secrets,
    bool print_pretty_type_names,
    IdentifierQuotingRule identifier_quoting_rule,
    IdentifierQuotingStyle identifier_quoting_style) const
{
    WriteBufferFromOwnString buf;
    FormatSettings settings(one_line);
    settings.show_secrets = show_secrets;
    settings.print_pretty_type_names = print_pretty_type_names;
    settings.identifier_quoting_rule = identifier_quoting_rule;
    settings.identifier_quoting_style = identifier_quoting_style;
    format(buf, settings);
    return wipeSensitiveDataAndCutToLength(buf.str(), max_length, !show_secrets);
}

String IAST::formatForLogging(size_t max_length) const
{
    return formatWithPossiblyHidingSensitiveData(
        /*max_length=*/max_length,
        /*one_line=*/true,
        /*show_secrets=*/false,
        /*print_pretty_type_names=*/false,
        /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
        /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks);
}

String IAST::formatForErrorMessage() const
{
    return formatWithPossiblyHidingSensitiveData(
        /*max_length=*/0,
        /*one_line=*/true,
        /*show_secrets=*/false,
        /*print_pretty_type_names=*/false,
        /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
        /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks);
}

String IAST::formatWithSecretsOneLine() const
{
    return formatWithPossiblyHidingSensitiveData(
        /*max_length=*/0,
        /*one_line=*/true,
        /*show_secrets=*/true,
        /*print_pretty_type_names=*/false,
        /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
        /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks);
}

String IAST::formatWithSecretsMultiLine() const
{
    return formatWithPossiblyHidingSensitiveData(
        /*max_length=*/0,
        /*one_line=*/false,
        /*show_secrets=*/true,
        /*print_pretty_type_names=*/false,
        /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
        /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks);
}

bool IAST::childrenHaveSecretParts() const
{
    for (const auto & child : children)
    {
        if (child->hasSecretParts())
            return true;
    }
    return false;
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


String IAST::getColumnNameWithoutAlias() const
{
    WriteBufferFromOwnString write_buffer;
    appendColumnNameWithoutAlias(write_buffer);
    return write_buffer.str();
}


void IAST::FormatSettings::writeIdentifier(WriteBuffer & ostr, const String & name, bool ambiguous) const
{
    checkIdentifier(name);
    bool must_quote
        = (identifier_quoting_rule == IdentifierQuotingRule::Always
           || (ambiguous && identifier_quoting_rule == IdentifierQuotingRule::WhenNecessary));

    if (identifier_quoting_rule == IdentifierQuotingRule::UserDisplay && !must_quote)
    {
        // Quote `name` if it is one of the keywords when `identifier_quoting_rule` is `IdentifierQuotingRule::UserDisplay`
        const auto & keyword_set = getKeyWordSet();
        must_quote = keyword_set.contains(Poco::toUpper(name));
    }

    switch (identifier_quoting_style)
    {
        case IdentifierQuotingStyle::Backticks:
        {
            if (must_quote)
                writeBackQuotedString(name, ostr);
            else
                writeProbablyBackQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::DoubleQuotes:
        {
            if (must_quote)
                writeDoubleQuotedString(name, ostr);
            else
                writeProbablyDoubleQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::BackticksMySQL:
        {
            if (must_quote)
                writeBackQuotedStringMySQL(name, ostr);
            else
                writeProbablyBackQuotedStringMySQL(name, ostr);
            break;
        }
    }
}

void IAST::FormatSettings::checkIdentifier(const String & name) const
{
    if (enforce_strict_identifier_format)
    {
        bool is_word_char_identifier = std::all_of(name.begin(), name.end(), isWordCharASCII);
        if (!is_word_char_identifier)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Identifier '{}' contains characters other than alphanumeric and cannot be when enforce_strict_identifier_format is enabled",
                name);
        }
    }
}

void IAST::dumpTree(WriteBuffer & ostr, size_t indent) const
{
    String indent_str(indent, '-');
    ostr << indent_str << getID() << ", ";
    writePointerHex(this, ostr);
    writeChar('\n', ostr);
    for (const auto & child : children)
    {
        if (!child)
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_AST, "Can't dump a nullptr child");
        child->dumpTree(ostr, indent + 1);
    }
}

std::string IAST::dumpTree(size_t indent) const
{
    WriteBufferFromOwnString wb;
    dumpTree(wb, indent);
    return wb.str();
}

/// Decide how to emit `parenthesized` parens. When the node has an alias and we are not in an
/// operator-chain context (`frame.need_parens == false`), defer to `ASTWithAlias::formatImpl` so
/// it can emit `(expr) AS alias` instead of `(expr AS alias)` — only the former re-formats to
/// itself and keeps the format-parse-format round-trip stable.
///
/// In operator-chain context (`frame.need_parens == true`) we keep the parens here so the output
/// is `(expr AS alias)`, because the parser would not accept `(expr) AS alias OP rhs` at the top
/// level of a SELECT element / WHERE clause (the alias terminates the SELECT element parser).
static bool decideParensEmission(const IAST & node, IAST::FormatStateStacked & frame)
{
    const bool parens = node.isParenthesized() && !frame.wrapped_in_parens;
    frame.wrapped_in_parens = false;
    if (!parens)
        return false;

    if (!frame.need_parens)
    {
        if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(&node);
            with_alias && !with_alias->alias.empty() && !dynamic_cast<const ASTSubquery *>(&node))
        {
            /// Skip the deferral for `ASTSubquery`: its `formatImplWithoutAlias` already emits
            /// `(SELECT ...)` itself, so deferring would produce `((SELECT ...)) AS alias`,
            /// and the parser collapses `((SELECT ...))` to a non-parenthesized subquery,
            /// breaking the round-trip. The default `(formatImpl-output)` wrapping in
            /// `IAST::format` produces `((SELECT ...) AS alias)` which round-trips cleanly.
            frame.parenthesize_alias_inner_only = true;
            frame.current_function = nullptr;
            frame.list_element_index = 0;
            return false;
        }
    }

    /// A multi-element tuple literal naturally formats as `(elem, elem, ...)` — the
    /// parens are part of the value's representation, not grouping parens. Emitting the
    /// `parenthesized` flag's parens on top of that would produce `((1, 2))` for inputs
    /// like `(((1), (2)))` or `NOT ((1, 1, 1))`, where the canonical form is a single
    /// pair. Suppress them here. The aliased-tuple case (`((1, 2)) AS a`) is already
    /// handled by the alias-deferral branch above and remains unaffected.
    if (const auto * literal = dynamic_cast<const ASTLiteral *>(&node);
        literal && literal->value.getType() == Field::Types::Tuple
            && literal->value.safeGet<Tuple>().size() > 1)
    {
        return false;
    }

    /// `ASTSubquery` without an alias always emits its own enclosing `(SELECT ...)` parens.
    /// Adding the `parenthesized` flag's parens on top would produce `((SELECT ...))`,
    /// which the parser collapses back to a non-parenthesized subquery, breaking the
    /// format-parse-format round-trip. The aliased case is different: there `((SELECT ...) AS alias)`
    /// is the canonical form (the alias-deferral branch above explicitly skips `ASTSubquery` so
    /// the outer parens are emitted here instead), so we only suppress when the alias is empty.
    if (const auto * subquery = dynamic_cast<const ASTSubquery *>(&node); subquery && subquery->alias.empty())
        return false;

    frame.need_parens = false;
    frame.current_function = nullptr;
    frame.list_element_index = 0;
    return true;
}

void IAST::format(WriteBuffer & ostr, const FormatSettings & settings) const
{
    FormatState state;
    FormatStateStacked frame;
    const bool parens = decideParensEmission(*this, frame);
    if (parens)
        ostr.write('(');
    formatImpl(ostr, settings, state, std::move(frame));
    if (parens)
        ostr.write(')');
}

void IAST::format(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const bool parens = decideParensEmission(*this, frame);
    if (parens)
        ostr.write('(');
    formatImpl(ostr, settings, state, std::move(frame));
    if (parens)
        ostr.write(')');
}

void IAST::format(FormattingBuffer out) const
{
    const bool parens = decideParensEmission(*this, out.frame);
    if (parens)
        out.ostr.write('(');
    formatImpl(out.ostr, out.settings, out.state, out.frame);
    if (parens)
        out.ostr.write(')');
}

void IAST::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    formatImpl(FormattingBuffer{ostr, settings, state, std::move(frame)});
}

void IAST::formatImpl(FormattingBuffer /*out*/) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown element in AST: {}", getID());
}

}
