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
		block_pos(0), block_size(DBMS_DEFAULT_HASHING_BLOCK_SIZE), state(0, 0), in(in_), ignore_before_this(nullptr)
	{
		working_buffer = in.buffer();

		/// если какая то часть данных уже была прочитана до нас, то не дадим этим данным повлиять на хэш
		if (in.position() != in.buffer().begin())
			ignore_before_this = in.position();
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

	bool nextImpl() override
	{
		size_t len = working_buffer.size();
		Position data = working_buffer.begin();

		/// корректировка на данные прочитанные до нас
		if (ignore_before_this)
		{
			len -= ignore_before_this - working_buffer.begin();
			data = ignore_before_this;
			ignore_before_this = nullptr;
		}

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
					memcpy(&memory[block_pos], data, len);
					append(&memory[0]);
					len -= n;
					data += n;
					block_pos = 0;
				}

				while (len >= block_pos)
				{
					append(data);
					len -= block_pos;
					data += block_pos;
				}

				if (len)
				{
					memcpy(&memory[0], data, len);
					block_pos = len;
				}
			}
		}

		bool res = in.next();
		working_buffer = in.buffer();
		return res;
	}

private:
	size_t block_pos;
	size_t block_size;
	uint128 state;
	ReadBuffer & in;

	/// игнорируем уже прочитанные данные
	Position ignore_before_this;
};
}
