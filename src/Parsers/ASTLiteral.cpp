#include <Common/SipHash.h>
#include <Common/checkStackSize.h>
#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorHash.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
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

void ASTLiteral::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Literal");
    w.writeFieldValue("value", value);
    w.writeAlias(*this);
}

void ASTLiteral::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    /// A `Literal` always requires an explicit `value` key (a NULL literal is written
    /// with an explicit `value`), so reject input that omits it instead of silently
    /// deserializing as a NULL literal, which the SQL parser cannot produce.
    if (!r.has("value"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing required 'value' key for Literal during AST JSON deserialization");
    value = r.readField("value");
    r.readAlias(*this);
}

String ASTLiteral::getID(char delim) const
{
    return "Literal" + (delim + applyVisitor(FieldVisitorDump(), value));
}

ASTPtr ASTLiteral::clone() const
{
    /// The copy constructor clears the token-info bit - see `ASTLiteral(const ASTLiteral &)`.
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

/// Base for the visitors that print a `Field` as a literal of a particular external dialect.
///
/// Everything except strings and containers is printed exactly like `FieldVisitorToString` does.
/// Container literals (`Tuple` / `Object`) are handled here so that the visitor recurses into
/// itself instead of falling back to `FieldVisitorToString`: once a target dialect is selected,
/// nested strings (e.g. the elements of an `IN` tuple) have to stay in that dialect all the way
/// down. Literals that only have a ClickHouse-specific text form (`Array` / `Map`, and tuples with
/// fewer than two elements, which can only be written as `tuple(...)`) are rejected: the external
/// database would fail to parse them, or worse, parse them into something else. Normally such
/// literals never reach the formatting stage (`isCompatible` keeps the predicates that carry them
/// out of the pushed-down query), so throwing here fails a query that would otherwise be sent to
/// the external database as broken SQL - e.g. a user-provided `(SELECT ...)` table argument, which
/// is formatted from the raw AST. `Derived` only has to provide `operator()` for `String` and
/// a `dialect_name` constant.
template <typename Derived>
class FieldVisitorToStringForDialect : public StaticVisitor<String>
{
public:
    template <typename T>
    String operator() (const T & x) const { return visitor(x); }

    [[noreturn]] String operator() (const Array &) const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot format an Array literal for {}: it has no syntax for such literals; "
            "the predicate can only be evaluated by ClickHouse", Derived::dialect_name);
    }

    [[noreturn]] String operator() (const Map &) const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot format a Map literal for {}: it has no syntax for such literals; "
            "the predicate can only be evaluated by ClickHouse", Derived::dialect_name);
    }

    String operator() (const Tuple & x) const
    {
        /// A tuple with fewer than two elements has no plain parenthesized form: it could only be
        /// written back with the explicit `tuple` function, which is ClickHouse syntax that the
        /// external database does not understand.
        if (x.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot format a tuple with fewer than two elements for {}: it can only be written "
                "in ClickHouse-specific syntax; the predicate can only be evaluated by ClickHouse",
                Derived::dialect_name);
        return formatContainer(x, "(", ")");
    }

    String operator() (const Object & x) const
    {
        checkStackSize();
        /// Like `FieldVisitorToString`: an Object is written as a string containing valid JSON,
        /// but the string itself has to be quoted with the rules of the target dialect.
        return derived()(convertObjectToString(x));
    }

private:
    FieldVisitorToString visitor;

    const Derived & derived() const { return static_cast<const Derived &>(*this); }

    template <typename Container>
    String formatContainer(const Container & x, const char * prefix, const char * suffix) const
    {
        checkStackSize();

        WriteBufferFromOwnString wb;
        wb << prefix;
        for (auto it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb << ", ";
            wb << applyVisitor(derived(), *it);
        }
        wb << suffix;
        return wb.str();
    }
};

/// Like `FieldVisitorToString`, but strings are escaped so that PostgreSQL reads back exactly the
/// original bytes (`writeQuotedStringPostgreSQLLossless`).
class FieldVisitorToStringPostgreSQL : public FieldVisitorToStringForDialect<FieldVisitorToStringPostgreSQL>
{
public:
    static constexpr const char * dialect_name = "PostgreSQL";

    using FieldVisitorToStringForDialect<FieldVisitorToStringPostgreSQL>::operator();

    String operator() (const String & x) const
    {
        /// A NUL byte cannot appear in a PostgreSQL string value (see `writeQuotedStringPostgreSQLLossless`).
        /// Predicates with such literals are normally not pushed down (`isCompatible` rejects them), so
        /// reaching here means we are about to emit a literal that cannot match: fail explicitly rather
        /// than silently produce wrong results.
        if (x.contains('\0'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot push down a predicate to PostgreSQL: a string literal contains a NUL byte, "
                "which cannot be represented in a PostgreSQL string value");

        WriteBufferFromOwnString wb;
        writeQuotedStringPostgreSQLLossless(x, wb);
        return wb.str();
    }
};

/// Like `FieldVisitorToString`, but strings are escaped with SQLite rules (`writeQuotedStringSQLite`).
class FieldVisitorToStringSQLite : public FieldVisitorToStringForDialect<FieldVisitorToStringSQLite>
{
public:
    static constexpr const char * dialect_name = "SQLite";

    using FieldVisitorToStringForDialect<FieldVisitorToStringSQLite>::operator();

    String operator() (const String & x) const
    {
        /// A NUL byte cannot be represented in a SQLite string literal (see `writeQuotedStringSQLite`).
        /// Predicates with such literals are normally not pushed down (`isCompatible` rejects them), so
        /// reaching here means we are about to emit a literal that cannot match: fail explicitly rather
        /// than silently produce wrong results.
        if (x.contains('\0'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot push down a predicate to SQLite: a string literal contains a NUL byte, "
                "which cannot be represented in a SQLite string literal");

        WriteBufferFromOwnString wb;
        writeQuotedStringSQLite(x, wb);
        return wb.str();
    }
};

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
