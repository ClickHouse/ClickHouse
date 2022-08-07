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
    extern const int UNKNOWN_ELEMENT_IN_AST;
}


const char * IAST::hilite_keyword      = "\033[1m";
const char * IAST::hilite_identifier   = "\033[0;36m";
const char * IAST::hilite_function     = "\033[0;33m";
const char * IAST::hilite_operator     = "\033[1;33m";
const char * IAST::hilite_alias        = "\033[0;32m";
const char * IAST::hilite_substitution = "\033[1;36m";
const char * IAST::hilite_none         = "\033[0m";


IAST::~IAST()
{
    /** Create intrusive linked list of children to delete.
      * Each ASTPtr child contains pointer to next child to delete.
      */
    ASTPtr delete_list_head_holder = nullptr;
    bool delete_directly = next_to_delete_list_head == nullptr;
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
        auto next_node_to_delete = std::move(delete_list_head_reference->next_to_delete);

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
        {
            ASTPtr child_to_delete;
            child_to_delete.swap(delete_list_head_reference);
            child_to_delete->next_to_delete_list_head = &delete_list_head_reference;
        }

        /** Here we have 2 cases:
          * 1. Child during deletion does not move any node into list, then just replace current head with next node to delete.
          * 2. Child during deletion moved some nodes into hist, then iterate list and add next to delete node in list tail.
          */
        if (delete_list_head_reference == nullptr)
        {
            delete_list_head_reference = std::move(next_node_to_delete);
        }
        else
        {
            ASTPtr delete_list_tail = delete_list_head_reference;

            while (delete_list_tail)
            {
                if (delete_list_tail->next_to_delete == nullptr)
                {
                    delete_list_tail->next_to_delete = std::move(next_node_to_delete);
                    break;
                }

                delete_list_tail = delete_list_tail->next_to_delete;
            }
        }
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
        throw Exception("AST is too big. Maximum: " + toString(max_size), ErrorCodes::TOO_BIG_AST);

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

std::string IAST::formatForErrorMessage() const
{
    WriteBufferFromOwnString buf;
    format(FormatSettings(buf, true /* one line */));
    return buf.str();
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


void IAST::FormatSettings::writeIdentifier(const String & name) const
{
    switch (identifier_quoting_style)
    {
        case IdentifierQuotingStyle::None:
        {
            if (always_quote_identifiers)
                throw Exception("Incompatible arguments: always_quote_identifiers = true && identifier_quoting_style == IdentifierQuotingStyle::None",
                    ErrorCodes::BAD_ARGUMENTS);
            writeString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::Backticks:
        {
            if (always_quote_identifiers)
                writeBackQuotedString(name, ostr);
            else
                writeProbablyBackQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::DoubleQuotes:
        {
            if (always_quote_identifiers)
                writeDoubleQuotedString(name, ostr);
            else
                writeProbablyDoubleQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::BackticksMySQL:
        {
            if (always_quote_identifiers)
                writeBackQuotedStringMySQL(name, ostr);
            else
                writeProbablyBackQuotedStringMySQL(name, ostr);
            break;
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
        if (!child) throw Exception("Can't dump nullptr child", ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
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
