#pragma once

#include <city.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/HashingWriteBuffer.h>

namespace DB
{
/*
 * Считает хэш от прочитанных данных. При чтении данные записываются во вложенный ReadBuffer.
 * Мелкие кусочки копируются в собственную память.
 */
class HashingReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
	HashingReadBuffer(ReadBuffer & in_, size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE) :
		block_pos(0), block_size(DBMS_DEFAULT_HASHING_BLOCK_SIZE), state(0, 0), in(in_)
	{
		working_buffer = in.buffer();
		pos = in.position();

		/// считаем хэш от уже прочитанных данных
		if (working_buffer.size())
		{
			calculateHash(pos, working_buffer.end() - pos);
		}
	}

	uint128 getHash()
	{
		if (block_pos)
			return CityHash128WithSeed(&memory[0], block_pos, state);
		else
			return state;
	}

private:
	/// вычисление хэша зависит от разбиения по блокам
	/// поэтому нужно вычислить хэш от n полных кусочков и одного неполного
	void append(Position data)
	{
		state = CityHash128WithSeed(data, block_size, state);
	}

	void calculateHash(Position data, size_t len)
	{
		if (len)
		{
			/// если данных меньше, чем block_size то сложим их в свой буффер и посчитаем от них hash позже
			if (block_pos + len < block_size)
			{
				memcpy(&memory[block_pos], data, len);
				block_pos += len;
			}
			else
			{
				/// если в буффер уже что-то записано, то
				if (block_pos)
				{
					size_t n = block_size - block_pos;
					memcpy(&memory[block_pos], data, n);
					append(&memory[0]);
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

				if (len)
				{
					memcpy(&memory[0], data, len);
					block_pos = len;
				}
			}
		}
	}

	bool nextImpl() override
	{
		in.position() = pos;
		bool res = in.next();
		working_buffer = in.buffer();
		pos = in.position();

		calculateHash(working_buffer.begin(), working_buffer.size());

		return res;
	}

private:
	size_t block_pos;
	size_t block_size;
	uint128 state;
	ReadBuffer & in;
};
}
