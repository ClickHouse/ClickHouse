#include <Core/UUID.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>


namespace DB
{

template <typename T>
static inline String formatQuoted(T x)
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline String formatQuotedWithPrefix(T x, const char * prefix)
{
    WriteBufferFromOwnString wb;
    wb.write(prefix, strlen(prefix));
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
String FieldVisitorDump::operator() (const UInt128 & x) const { return formatQuotedWithPrefix(UUID(x), "UUID_"); }


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

String FieldVisitorDump::operator() (const Tuple & x_def) const
{
    auto & x = x_def.toUnderType();
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


/** In contrast to writeFloatText (and writeQuoted),
  *  even if number looks like integer after formatting, prints decimal point nevertheless (for example, Float64(1) is printed as 1.).
  * - because resulting text must be able to be parsed back as Float64 by query parser (otherwise it will be parsed as integer).
  *
  * Trailing zeros after decimal point are omitted.
  *
  * NOTE: Roundtrip may lead to loss of precision.
  */
static String formatFloat(const Float64 x)
{
    DoubleConverter<true>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

    if (!result)
        throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    return { buffer, buffer + builder.position() };
}


String FieldVisitorToString::operator() (const Null &) const { return "NULL"; }
String FieldVisitorToString::operator() (const UInt64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Float64 & x) const { return formatFloat(x); }
String FieldVisitorToString::operator() (const String & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal32> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal64> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal128> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UInt128 & x) const { return formatQuoted(UUID(x)); }

String FieldVisitorToString::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << '[';
    for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToString::operator() (const Tuple & x_def) const
{
    auto & x = x_def.toUnderType();
    WriteBufferFromOwnString wb;

    wb << '(';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}


FieldVisitorHash::FieldVisitorHash(SipHash & hash) : hash(hash) {}

void FieldVisitorHash::operator() (const Null &) const
{
    UInt8 type = Field::Types::Null;
    hash.update(type);
}

void FieldVisitorHash::operator() (const UInt64 & x) const
{
    UInt8 type = Field::Types::UInt64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const UInt128 & x) const
{
    UInt8 type = Field::Types::UInt128;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int64 & x) const
{
    UInt8 type = Field::Types::Int64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Float64 & x) const
{
    UInt8 type = Field::Types::Float64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const String & x) const
{
    UInt8 type = Field::Types::String;
    hash.update(type);
    hash.update(x.size());
    hash.update(x.data(), x.size());
}

void FieldVisitorHash::operator() (const Array & x) const
{
    UInt8 type = Field::Types::Array;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal32> & x) const
{
    UInt8 type = Field::Types::Decimal32;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal64> & x) const
{
    UInt8 type = Field::Types::Decimal64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal128> & x) const
{
    UInt8 type = Field::Types::Decimal128;
    hash.update(type);
    hash.update(x);
}


}
