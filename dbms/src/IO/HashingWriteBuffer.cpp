#include <DB/IO/HashingWriteBuffer.h>
#include <iomanip>

/// UInt64 это 39 символов в 10 системе счисления
static const size_t UINT64_DECIMAL_SIZE = 39;
std::string uint128ToString(uint128 data)
{
	std::stringstream ss;
	ss << std::setw(UINT64_DECIMAL_SIZE) << std::setfill('0') << data.first << std::setw(UINT64_DECIMAL_SIZE) << std::setfill('0') << data.second;
	return ss.str();
}

std::ostream & operator<<(std::ostream & os, const uint128 & data)
{
	os << uint128ToString(data);
	return os;
}

std::istream & operator>>(std::istream & is, uint128 & data)
{
	std::vector<char> buffer(UINT64_DECIMAL_SIZE);
	is.read(buffer.data(), UINT64_DECIMAL_SIZE);
	data.first = DB::parse<UInt64>(buffer.data(), UINT64_DECIMAL_SIZE);

	if (!is)
		throw DB::Exception(std::string("Fail to parse uint128 from ") + buffer.data());

	is.read(buffer.data(), UINT64_DECIMAL_SIZE);
	data.first = DB::parse<UInt64>(buffer.data(), UINT64_DECIMAL_SIZE);

	if (!is)
		throw DB::Exception(std::string("Fail to parse uint128 from ") + buffer.data());

	return is;
}
