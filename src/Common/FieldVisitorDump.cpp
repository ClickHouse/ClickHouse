#include <Common/FieldVisitorDump.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

template <typename T>
static inline String formatQuotedWithPrefix(T x, const char * prefix)
{
    WriteBufferFromOwnString wb;
    writeCString(prefix, wb);
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline void writeQuoted(const DecimalField<T> & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x.getValue(), x.getScale(), buf);
    writeChar('\'', buf);
}

String FieldVisitorDump::operator() (const Null &) const { return "NULL"; }
String FieldVisitorDump::operator() (const UInt64 & x) const { return formatQuotedWithPrefix(x, "UInt64_"); }
String FieldVisitorDump::operator() (const Int64 & x) const { return formatQuotedWithPrefix(x, "Int64_"); }
String FieldVisitorDump::operator() (const Float64 & x) const { return formatQuotedWithPrefix(x, "Float64_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal32> & x) const { return formatQuotedWithPrefix(x, "Decimal32_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal64> & x) const { return formatQuotedWithPrefix(x, "Decimal64_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal128> & x) const { return formatQuotedWithPrefix(x, "Decimal128_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal256> & x) const { return formatQuotedWithPrefix(x, "Decimal256_"); }
String FieldVisitorDump::operator() (const UInt128 & x) const { return formatQuotedWithPrefix(x, "UInt128_"); }
String FieldVisitorDump::operator() (const UInt256 & x) const { return formatQuotedWithPrefix(x, "UInt256_"); }
String FieldVisitorDump::operator() (const Int128 & x) const { return formatQuotedWithPrefix(x, "Int128_"); }
String FieldVisitorDump::operator() (const Int256 & x) const { return formatQuotedWithPrefix(x, "Int256_"); }
String FieldVisitorDump::operator() (const UUID & x) const { return formatQuotedWithPrefix(x, "UUID_"); }


String FieldVisitorDump::operator() (const String & x) const
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

String FieldVisitorDump::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Array_[";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorDump::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Tuple_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Map_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const AggregateFunctionStateData & x) const
{
    WriteBufferFromOwnString wb;
    wb << "AggregateFunctionState_(";
    writeQuoted(x.name, wb);
    wb << ", ";
    writeQuoted(x.data, wb);
    wb << ')';
    return wb.str();
}

}

