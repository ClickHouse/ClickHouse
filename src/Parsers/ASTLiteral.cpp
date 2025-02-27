#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorHash.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTLiteral::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    const char * prefix = "Literal_";
    hash_state.update(prefix, strlen(prefix));
    applyVisitor(FieldVisitorHash(hash_state), value);
    if (!ignore_aliases)
        ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);
}

ASTPtr ASTLiteral::clone() const
{
    auto res = std::make_shared<ASTLiteral>(*this);
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
    if (use_legacy_column_name_of_tuple)
    {
        appendColumnNameImplLegacy(ostr);
        return;
    }

    /// 100 - just arbitrary value.
    constexpr auto min_elements_for_hashing = 100;

    /// Special case for very large arrays and tuples. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    auto type = value.getType();
    if ((type == Field::Types::Array && value.safeGet<const Array &>().size() > min_elements_for_hashing)
        || (type == Field::Types::Tuple && value.safeGet<const Tuple &>().size() > min_elements_for_hashing))
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low, high;
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
    if ((type == Field::Types::Array && value.safeGet<const Array &>().size() > min_elements_for_hashing))
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low, high;
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

void ASTLiteral::formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    if (settings.literal_escaping_style == LiteralEscapingStyle::Regular)
        ostr << applyVisitor(FieldVisitorToString(), value);
    else
        ostr << applyVisitor(FieldVisitorToStringPostgreSQL(), value);
}

}
