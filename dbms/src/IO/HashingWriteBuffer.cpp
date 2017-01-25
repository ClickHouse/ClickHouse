#include <DB/IO/HashingWriteBuffer.h>
#include <iomanip>
#include <city.h>

namespace DB
{

/// вычисление хэша зависит от разбиения по блокам
/// поэтому нужно вычислить хэш от n полных кусочков и одного неполного
template <class Buffer>
void IHashingBuffer<Buffer>::calculateHash(DB::BufferBase::Position data, size_t len)
{
	if (len)
	{
		/// если данных меньше, чем block_size то сложим их в свой буффер и посчитаем от них hash позже
		if (block_pos + len < block_size)
		{
			memcpy(&BufferWithOwnMemory<Buffer>::memory[block_pos], data, len);
			block_pos += len;
		}
		else
		{
			/// если в буффер уже что-то записано, то допишем его
			if (block_pos)
			{
				size_t n = block_size - block_pos;
				memcpy(&BufferWithOwnMemory<Buffer>::memory[block_pos], data, n);
				append(&BufferWithOwnMemory<Buffer>::memory[0]);
				len -= n;
				data += n;
				block_pos = 0;
			}

			while (len >= block_size)
			{
				append(data);
				len -= block_size;
				data += block_size;
			}

			/// запишем остаток в свой буфер
			if (len)
			{
				memcpy(&BufferWithOwnMemory<Buffer>::memory[0], data, len);
				block_pos = len;
			}
		}
	}
}

template class IHashingBuffer<DB::ReadBuffer>;
template class IHashingBuffer<DB::WriteBuffer>;

}

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
