#include <cmath>

#include <Common/formatReadable.h>
#include <IO/DoubleConverter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    }
}

static void formatReadable(double size, DB::WriteBuffer & out,
    int precision, const char ** units, size_t units_size, double delimiter)
{
    size_t i = 0;
    for (; i + 1 < units_size && fabs(size) >= delimiter; ++i)
        size /= delimiter;

    DB::DoubleConverter<false>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto & converter = DB::DoubleConverter<false>::instance();

    auto result = converter.ToFixed(size, precision, &builder);

    if (!result)
        result = converter.ToShortest(size, &builder);

    if (!result)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print float or double number");

    out.write(buffer, builder.position());
    writeCString(units[i], out);
}


void formatReadableSizeWithBinarySuffix(double value, DB::WriteBuffer & out, int precision)
{
    const char * units[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1024);
}

std::string formatReadableSizeWithBinarySuffix(double value, int precision)
{
    DB::WriteBufferFromOwnString out;
    formatReadableSizeWithBinarySuffix(value, out, precision);
    return out.str();
}


void formatReadableSizeWithDecimalSuffix(double value, DB::WriteBuffer & out, int precision)
{
    const char * units[] = {" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableSizeWithDecimalSuffix(double value, int precision)
{
    DB::WriteBufferFromOwnString out;
    formatReadableSizeWithDecimalSuffix(value, out, precision);
    return out.str();
}


void formatReadableQuantity(double value, DB::WriteBuffer & out, int precision)
{
    const char * units[] = {"", " thousand", " million", " billion", " trillion", " quadrillion",
        " quintillion", " sextillion", " septillion", " octillion", " nonillion", " decillion",
        " undecillion", " duodecillion", " tredecillion", " quattuordecillion", " quindecillion",
        " sexdecillion", " septendecillion", " octodecillion", " novemdecillion", " vigintillion"};

    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableQuantity(double value, int precision)
{
    DB::WriteBufferFromOwnString out;
    formatReadableQuantity(value, out, precision);
    return out.str();
}

void formatReadableTime(double ns, DB::WriteBuffer & out, int precision)
{
    const char * units[] = {" ns", " us", " ms", " s"};
    formatReadable(ns, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableTime(double ns, int precision)
{
    DB::WriteBufferFromOwnString out;
    formatReadableTime(ns, out, precision);
    return out.str();
}
