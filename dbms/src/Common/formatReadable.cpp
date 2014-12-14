#include <cmath>
#include <sstream>
#include <iomanip>

#include <DB/Common/formatReadable.h>


static std::string formatReadable(double size, int precision, const char ** units, size_t units_size, double delimiter)
{
	size_t i = 0;
	for (; i + 1 < units_size && fabs(size) >= delimiter; ++i)
		size /= delimiter;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(precision) << size << units[i];
    return ss.str();
}


std::string formatReadableSizeWithBinarySuffix(double value, int precision)
{
	const char * units[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"};
	return formatReadable(value, precision, units, sizeof(units) / sizeof(units[0]), 1024);
}

std::string formatReadableSizeWithDecimalSuffix(double value, int precision)
{
	const char * units[] = {" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"};
	return formatReadable(value, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableQuantity(double value, int precision)
{
	const char * units[] = {"", " thousand", " million", " billion", " trillion", " quadrillion"};
	return formatReadable(value, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}
