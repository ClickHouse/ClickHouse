#include <cmath>
#include <sstream>
#include <iomanip>

#include <DB/Common/formatReadable.h>
#include <DB/IO/DoubleConverter.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>


static void formatReadable(double size, DB::WriteBuffer & out, int precision, const char ** units, size_t units_size, double delimiter)
{
	size_t i = 0;
	for (; i + 1 < units_size && fabs(size) >= delimiter; ++i)
		size /= delimiter;

	char tmp[25];
	double_conversion::StringBuilder builder{tmp, sizeof(tmp)};

	const auto result = DB::getDoubleToStringConverter<false>().ToFixed(size, precision, &builder);

	if (!result)
		throw DB::Exception("Cannot print float or double number", DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	out.write(tmp, builder.position());
	writeCString(units[i], out);
}


void formatReadableSizeWithBinarySuffix(double value, DB::WriteBuffer & out, int precision)
{
	const char * units[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"};
	formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1024);
}

std::string formatReadableSizeWithBinarySuffix(double value, int precision)
{
	std::string res;
	DB::WriteBufferFromString out(res);
	formatReadableSizeWithBinarySuffix(value, out, precision);
    return res;
}


void formatReadableSizeWithDecimalSuffix(double value, DB::WriteBuffer & out, int precision)
{
	const char * units[] = {" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"};
	formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableSizeWithDecimalSuffix(double value, int precision)
{
	std::string res;
	DB::WriteBufferFromString out(res);
	formatReadableSizeWithDecimalSuffix(value, out, precision);
    return res;
}


void formatReadableQuantity(double value, DB::WriteBuffer & out, int precision)
{
	const char * units[] = {"", " thousand", " million", " billion", " trillion", " quadrillion"};
	formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableQuantity(double value, int precision)
{
	std::string res;
	DB::WriteBufferFromString out(res);
	formatReadableQuantity(value, out, precision);
    return res;
}
