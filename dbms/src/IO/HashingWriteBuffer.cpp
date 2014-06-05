#include <DB/IO/HashingWriteBuffer.h>
#include <iomanip>

std::ostream & operator<<(std::ostream & os, const uint128 & data)
{
	/// UInt64 это 39 символов в 10 системе счисления
	os << std::setw(39) << std::setfill('0') << data.first << std::setw(39) << std::setfill('0') << data.second;
	return os;
}

std::istream & operator>>(std::istream & is, uint128 & data)
{
	std::vector<char> buffer(39);
	is.read(buffer.data(), 39);
	data.first = DB::parse<UInt64>(buffer.data(), 39);

	if (!is)
		throw DB::Exception(std::string("Fail to parse uint128 from ") + buffer.data());

	is.read(buffer.data(), 39);
	data.first = DB::parse<UInt64>(buffer.data(), 39);

	if (!is)
		throw DB::Exception(std::string("Fail to parse uint128 from ") + buffer.data());

	return is;
}
