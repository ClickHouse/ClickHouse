#include <Parsers/IAST.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_BIG_AST;
    extern const int TOO_DEEP_AST;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ELEMENT_IN_AST;
}


IAST::~IAST()
{
    /** Create intrusive linked list of children to delete.
      * Each ASTPtr child contains pointer to next child to delete.
      */
    ASTPtr delete_list_head_holder = nullptr;
    const bool delete_directly = next_to_delete_list_head == nullptr;
    ASTPtr & delete_list_head_reference = next_to_delete_list_head ? *next_to_delete_list_head : delete_list_head_holder;

    /// Move children into intrusive list
    for (auto & child : children)
    {
        /** If two threads remove ASTPtr concurrently,
          * it is possible that neither thead will see use_count == 1.
          * It is ok. Will need one more extra stack frame in this case.
          */
        if (child.use_count() != 1)
            continue;

        ASTPtr child_to_delete;
        child_to_delete.swap(child);

        if (!delete_list_head_reference)
        {
            /// Initialize list first time
            delete_list_head_reference = std::move(child_to_delete);
            continue;
        }

        ASTPtr previous_head = std::move(delete_list_head_reference);
        delete_list_head_reference = std::move(child_to_delete);
        delete_list_head_reference->next_to_delete = std::move(previous_head);
    }

    if (!delete_directly)
        return;

    while (delete_list_head_reference)
    {
        /** Extract child to delete from current list head.
          * Child will be destroyed at the end of scope.
          */
        ASTPtr child_to_delete;
        child_to_delete.swap(delete_list_head_reference);

        /// Update list head
        delete_list_head_reference = std::move(child_to_delete->next_to_delete);

        /** Pass list head into child before destruction.
          * It is important to properly handle cases where subclass has member same as one of its children.
          *
          * class ASTSubclass : IAST
          * {
          *     ASTPtr first_child; /// Same as first child
          * }
          *
          * In such case we must move children into list only in IAST destructor.
          * If we try to move child to delete children into list before subclasses desruction,
          * first child use count will be 2.
          */
        child_to_delete->next_to_delete_list_head = &delete_list_head_reference;
    }
}

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


IAST::Hash IAST::getTreeHash() const
{
    SipHash hash_state;
    updateTreeHash(hash_state);
    IAST::Hash res;
    hash_state.get128(res);
    return res;
}


void IAST::updateTreeHash(SipHash & hash_state) const
{
    updateTreeHashImpl(hash_state);
    hash_state.update(children.size());
    for (const auto & child : children)
        child->updateTreeHash(hash_state);
}


void IAST::updateTreeHashImpl(SipHash & hash_state) const
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

String IAST::formatWithSecretsHidden(size_t max_length, bool one_line) const
{
    WriteBufferFromOwnString buf;

    FormatSettings settings(one_line, false, IdentifierQuotingStyle::Backticks, false, false);
    format(FormattingBuffer(buf, settings));

    return wipeSensitiveDataAndCutToLength(buf.str(), max_length);
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

const char * IAST::FormattingBuffer::hilite_keyword      = "\033[1m";
const char * IAST::FormattingBuffer::hilite_identifier   = "\033[0;36m";
const char * IAST::FormattingBuffer::hilite_function     = "\033[0;33m";
const char * IAST::FormattingBuffer::hilite_operator     = "\033[1;33m";
const char * IAST::FormattingBuffer::hilite_alias        = "\033[0;32m";
const char * IAST::FormattingBuffer::hilite_substitution = "\033[1;36m";
const char * IAST::FormattingBuffer::hilite_metacharacter = "\033[1;35m";
const char * IAST::FormattingBuffer::hilite_none         = "\033[0m";

IAST::FormattingBuffer::Hiliter::Hiliter(DB::WriteBuffer & ostr_, bool hilite_, const char * hilite_type) : ostr(ostr_), hilite(hilite_)
{
    if (hilite)
        ostr << hilite_type;
}

IAST::FormattingBuffer::Hiliter::~Hiliter()
{
    if (hilite)
        ostr << IAST::FormattingBuffer::hilite_none;
}

IAST::FormattingBuffer::Hiliter IAST::FormattingBuffer::createHiliter(const char * hilite_type) const
{
    return {ostr, settings.hilite, hilite_type};
}

void IAST::FormattingBuffer::writePossiblyHilited(std::string_view str, const char * hilite_type) const
{
    Hiliter hiliter = createHiliter(hilite_type);
    ostr << str;
}

void IAST::FormattingBuffer::writeKeyword(std::string_view str) const
{
    writePossiblyHilited(str, IAST::FormattingBuffer::hilite_keyword);
}

void IAST::FormattingBuffer::writeFunction(std::string_view str) const
{
    writePossiblyHilited(str, IAST::FormattingBuffer::hilite_function);
}

void IAST::FormattingBuffer::writeOperator(std::string_view str) const
{
    writePossiblyHilited(str, IAST::FormattingBuffer::hilite_operator);
}

void IAST::FormattingBuffer::writeSubstitution(std::string_view str) const
{
    writePossiblyHilited(str, IAST::FormattingBuffer::hilite_substitution);
}

void IAST::FormattingBuffer::writeStringLiteralWithMetacharacters(const String & str, const char * metacharacters) const
{
    if (!settings.hilite)
    {
        ostr << str;
        return;
    }

    unsigned escaping = 0;
    for (auto c : str)
    {
        if (c == '\\')
        {
            ostr << c;
            if (escaping == 2)
                escaping = 0;
            ++escaping;
        }
        else if (nullptr != strchr(metacharacters, c))
        {
            if (escaping == 2) /// Properly escaped metacharacter
                ostr << c;
            else /// Unescaped metacharacter
                ostr << hilite_metacharacter << c << hilite_none;
            escaping = 0;
        }
        else
        {
            ostr << c;
            escaping = 0;
        }
    }
}

void IAST::FormattingBuffer::writeIdentifierOrAlias(const String & name, bool should_hilite_as_alias) const
{
    const char * hilite_type = should_hilite_as_alias ? IAST::FormattingBuffer::hilite_alias : IAST::FormattingBuffer::hilite_identifier;
    Hiliter hiliter = createHiliter(hilite_type);

    switch (settings.identifier_quoting_style)
    {
        case IdentifierQuotingStyle::None:
        {
            if (settings.always_quote_identifiers)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Incompatible arguments: always_quote_identifiers = true && "
                                "identifier_quoting_style == IdentifierQuotingStyle::None");
            writeString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::Backticks:
        {
            if (settings.always_quote_identifiers)
                writeBackQuotedString(name, ostr);
            else
                writeProbablyBackQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::DoubleQuotes:
        {
            if (settings.always_quote_identifiers)
                writeDoubleQuotedString(name, ostr);
            else
                writeProbablyDoubleQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::BackticksMySQL:
        {
            if (settings.always_quote_identifiers)
                writeBackQuotedStringMySQL(name, ostr);
            else
                writeProbablyBackQuotedStringMySQL(name, ostr);
            break;
        }
    }
}

void IAST::FormattingBuffer::writeIdentifier(const String & name) const
{
    writeIdentifierOrAlias(name);
}

void IAST::FormattingBuffer::writeAlias(const String & name) const
{
    writeIdentifierOrAlias(name, true);
}

void IAST::FormattingBuffer::writeProbablyBackQuotedIdentifier(const String & name) const
{
    Hiliter hiliter = createHiliter(hilite_identifier);
    writeProbablyBackQuotedString(name, ostr);
}

bool IAST::FormattingBuffer::isOneLine() const
{
    return settings.one_line;
}

void IAST::FormattingBuffer::nlOrWs() const
{
    ostr << (settings.one_line ? '\n' : ' ');
}

void IAST::FormattingBuffer::nlOrNothing() const
{
    ostr << (settings.one_line ? "\n" : "");
}

void IAST::FormattingBuffer::writeSecret(const String & secret) const
{
    ostr << (settings.show_secrets ? secret : "'[HIDDEN]'");
}

bool IAST::FormattingBuffer::shouldShowSecrets() const
{
    return settings.show_secrets;
}

int IAST::FormattingBuffer::writeIndent() const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * stacked_state->indent, ' ');
    ostr << indent_str;
}

bool IAST::FormattingBuffer::insertAlias(std::string alias, Hash printed_content) const
{
    return !state->printed_asts_with_alias.emplace(stacked_state->current_select, alias, printed_content).second;
}

void IAST::dumpTree(WriteBuffer & ostr, size_t indent) const
{
    String indent_str(indent, '-');
    ostr << indent_str << getID() << ", ";
    writePointerHex(this, ostr);
    writeChar('\n', ostr);
    for (const auto & child : children)
    {
        if (!child) throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_AST, "Can't dump nullptr child");
        child->dumpTree(ostr, indent + 1);
    }
}

std::string IAST::dumpTree(size_t indent) const
{
    WriteBufferFromOwnString wb;
    dumpTree(wb, indent);
    return wb.str();
}

}
