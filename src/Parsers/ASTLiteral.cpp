#include <Common/SipHash.h>
#include <Common/checkStackSize.h>
#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorHash.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTLiteral::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    const char * prefix = "Literal_";
    hash_state.update(prefix, strlen(prefix));
    applyVisitor(FieldVisitorHash(hash_state), value);
    if (!ignore_aliases)
        ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);
}

String ASTLiteral::getID(char delim) const
{
    return "Literal" + (delim + applyVisitor(FieldVisitorDump(), value));
}

ASTPtr ASTLiteral::clone() const
{
    auto res = make_intrusive<ASTLiteral>(*this);
    res->unique_column_name = {};
    return res;
}

namespace
{

/// Writes 'tuple' word before tuple literals for backward compatibility reasons.
class FieldVisitorToColumnName : public StaticVisitor<String>
{
public:
    template<typename T>
    String operator() (const T & x) const { return visitor(x); }

private:
    FieldVisitorToString visitor;
};

template<>
String FieldVisitorToColumnName::operator() (const Tuple & x) const
{
    checkStackSize();

    WriteBufferFromOwnString wb;

    wb << "tuple(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

}

void ASTLiteral::appendColumnNameImpl(WriteBuffer & ostr) const
{
    if (getUseLegacyColumnNameOfTuple())
    {
        appendColumnNameImplLegacy(ostr);
        return;
    }

    /// 100 - just arbitrary value.
    constexpr auto min_elements_for_hashing = 100;

    /// Special case for very large arrays and tuples. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    auto type = value.getType();
    if ((type == Field::Types::Array && value.safeGet<Array>().size() > min_elements_for_hashing)
        || (type == Field::Types::Tuple && value.safeGet<Tuple>().size() > min_elements_for_hashing))
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low = 0;
        UInt64 high = 0;
        hash.get128(low, high);

        writeCString(type == Field::Types::Array ? "__array_" : "__tuple_", ostr);
        writeText(low, ostr);
        ostr.write('_');
        writeText(high, ostr);
    }
    else
    {
        /// Shortcut for huge AST. The `FieldVisitorToString` becomes expensive
        /// for tons of literals as it creates temporary String.
        if (value.getType() == Field::Types::String)
        {
            writeQuoted(value.safeGet<String>(), ostr);
        }
        else
        {
            String column_name = applyVisitor(FieldVisitorToString(), value);
            writeString(column_name, ostr);
        }
    }
}

void ASTLiteral::appendColumnNameImplLegacy(WriteBuffer & ostr) const
{
    /// 100 - just arbitrary value.
    constexpr auto min_elements_for_hashing = 100;

    /// Special case for very large arrays. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    auto type = value.getType();
    if ((type == Field::Types::Array && value.safeGet<Array>().size() > min_elements_for_hashing))
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low = 0;
        UInt64 high = 0;
        hash.get128(low, high);

        writeCString("__array_", ostr);
        writeText(low, ostr);
        ostr.write('_');
        writeText(high, ostr);
    }
    else
    {
        String column_name = applyVisitor(FieldVisitorToColumnName(), value);
        writeString(column_name, ostr);
    }
}

/// Use different rules for escaping backslashes and quotes
class FieldVisitorToStringPostgreSQL : public StaticVisitor<String>
{
public:
    template<typename T>
    String operator() (const T & x) const { return visitor(x); }

private:
    FieldVisitorToString visitor;
};

template<>
String FieldVisitorToStringPostgreSQL::operator() (const String & x) const
{
    WriteBufferFromOwnString wb;
    writeQuotedStringPostgreSQL(x, wb);
    return wb.str();
}

/// Outputs a string as a standard SQL string literal: only the enclosing single quote is escaped, by
/// doubling it (''); every other byte, including backslashes and control characters, is emitted literally -
/// the rules of SQLite, where a string literal has no backslash escape sequences at all. A NUL byte cannot be
/// represented at all: the SQL text is passed to `sqlite3_exec`/`sqlite3_prepare` as NUL-terminated text and
/// would be silently truncated at the embedded NUL, so fail closed instead. Predicate pushdown filters such
/// literals out beforehand (see `transformQueryForExternalDatabase`), so this throws only for an explicit
/// user-written query - e.g. a `(SELECT ...)` argument - that no rewrite could represent faithfully.
static void writeQuotedStringStandardSQL(std::string_view ref, WriteBuffer & buf)
{
    if (ref.find('\0') != std::string_view::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "A string literal with an embedded NUL byte cannot be represented as standard SQL text");

    writeChar('\'', buf);
    for (char c : ref)
    {
        if (c == '\'')
            writeChar('\'', buf);
        writeChar(c, buf);
    }
    writeChar('\'', buf);
}

/// Escape string literals as standard SQL: only the quote is doubled, everything else stays literal.
/// Composite literals (Tuple/Array/Map) must recurse through this same visitor: a scalar `IN (...)` set is a
/// Tuple, so if composites fell back to the regular backslash-escaping `FieldVisitorToString` their nested
/// strings would be emitted with `\n`/`\t`/`\\` sequences. A pushed-down predicate such as
/// `s IN ('a\nb', 'plain')` would then compare against the wrong bytes and silently miss the matching row.
class FieldVisitorToStringStandardSQL : public StaticVisitor<String>
{
public:
    template<typename T>
    String operator() (const T & x) const { return visitor(x); }

    String operator() (const Array & x) const;
    String operator() (const Tuple & x) const;
    String operator() (const Map & x) const;

private:
    FieldVisitorToString visitor;
};

template<>
String FieldVisitorToStringStandardSQL::operator() (const String & x) const
{
    WriteBufferFromOwnString wb;
    writeQuotedStringStandardSQL(x, wb);
    return wb.str();
}

String FieldVisitorToStringStandardSQL::operator() (const Array & x) const
{
    checkStackSize();
    WriteBufferFromOwnString wb;

    wb << '[';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToStringStandardSQL::operator() (const Tuple & x) const
{
    checkStackSize();
    WriteBufferFromOwnString wb;

    /// For single-element tuples we must use the explicit tuple() function,
    /// or they will be parsed back as plain literals.
    if (x.size() > 1)
        wb << '(';
    else
        wb << "tuple(";

    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorToStringStandardSQL::operator() (const Map & x) const
{
    checkStackSize();
    WriteBufferFromOwnString wb;

    wb << '[';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

void ASTLiteral::formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    switch (settings.literal_escaping_style)
    {
        case LiteralEscapingStyle::Regular:
            ostr << applyVisitor(FieldVisitorToString(), value);
            break;
        case LiteralEscapingStyle::PostgreSQL:
            ostr << applyVisitor(FieldVisitorToStringPostgreSQL(), value);
            break;
        case LiteralEscapingStyle::StandardSQL:
            ostr << applyVisitor(FieldVisitorToStringStandardSQL(), value);
            break;
    }
}

}
