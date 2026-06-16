#include <Common/SipHash.h>
#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorHash.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


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

/// Like FieldVisitorToString, but strings are escaped with SQLite rules (writeQuotedStringSQLite).
/// Container literals (Array/Tuple/Map) are handled explicitly so that the visitor recurses into
/// itself: nested strings (e.g. the elements of an `IN` tuple) are escaped for SQLite too, instead
/// of falling back to the default backslash escaping of the embedded FieldVisitorToString.
class FieldVisitorToStringSQLite : public StaticVisitor<String>
{
public:
    template<typename T>
    String operator() (const T & x) const { return visitor(x); }

private:
    FieldVisitorToString visitor;
};

/// Forward declarations of the explicit specializations so that the container overloads below can
/// recurse into each other (e.g. a Tuple element that is itself a String or a nested Tuple)
/// regardless of definition order.
template<> String FieldVisitorToStringSQLite::operator() (const String & x) const;
template<> String FieldVisitorToStringSQLite::operator() (const Array & x) const;
template<> String FieldVisitorToStringSQLite::operator() (const Tuple & x) const;
template<> String FieldVisitorToStringSQLite::operator() (const Map & x) const;

template<>
String FieldVisitorToStringSQLite::operator() (const String & x) const
{
    /// A NUL byte cannot be represented in a SQLite string literal (see writeQuotedStringSQLite).
    /// Predicates with such literals are normally not pushed down (isCompatible rejects them), so
    /// reaching here means we are about to emit a literal that cannot match: fail explicitly rather
    /// than silently produce wrong results.
    if (x.find('\0') != String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot push down a predicate to SQLite: a string literal contains a NUL byte, "
            "which cannot be represented in a SQLite string literal");

    WriteBufferFromOwnString wb;
    writeQuotedStringSQLite(x, wb);
    return wb.str();
}

template<>
String FieldVisitorToStringSQLite::operator() (const Array & x) const
{
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

template<>
String FieldVisitorToStringSQLite::operator() (const Tuple & x) const
{
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

template<>
String FieldVisitorToStringSQLite::operator() (const Map & x) const
{
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
    if (settings.literal_escaping_style == LiteralEscapingStyle::Regular)
        ostr << applyVisitor(FieldVisitorToString(), value);
    else if (settings.literal_escaping_style == LiteralEscapingStyle::PostgreSQL)
        ostr << applyVisitor(FieldVisitorToStringPostgreSQL(), value);
    else
        ostr << applyVisitor(FieldVisitorToStringSQLite(), value);
}

}
