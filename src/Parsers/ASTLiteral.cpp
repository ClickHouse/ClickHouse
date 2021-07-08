#include <Common/SipHash.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTLiteral::updateTreeHashImpl(SipHash & hash_state) const
{
    const char * prefix = "Literal_";
    hash_state.update(prefix, strlen(prefix));
    applyVisitor(FieldVisitorHash(hash_state), value);
}

/// Writes 'tuple' word before tuple literals for backward compatibility reasons.
/// TODO: remove, when versions lower than 20.3 will be rarely used.
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

void ASTLiteral::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// 100 - just arbitrary value.
    constexpr auto min_elements_for_hashing = 100;

    /// Special case for very large arrays. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    /// TODO: Also do hashing for large tuples, when versions lower than 20.3 will be rarely used, because it breaks backward compatibility.
    auto type = value.getType();
    if ((type == Field::Types::Array && value.get<const Array &>().size() > min_elements_for_hashing))
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

void ASTLiteral::formatImplWithoutAlias(const FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << applyVisitor(FieldVisitorToString(), value);
}

}
