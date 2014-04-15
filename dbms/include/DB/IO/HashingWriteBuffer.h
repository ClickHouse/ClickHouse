#pragma once

#include <city.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>

#define DBMS_DEFAULT_HASHING_BLOCK_SIZE 2048ULL


namespace DB
{

/** Вычисляет хеш от записываемых данных и передает их в указанный WriteBuffer.
  * В качестве основного буфера используется буфер вложенного WriteBuffer.
  */
class HashingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
	WriteBuffer & out;

	size_t block_size;
	size_t block_pos;
	uint128 state;

	void append(Position data)
	{
		state = CityHash128WithSeed(data, block_size, state);
	}

	void nextImpl() override
	{
		size_t len = offset();

		if (len)
		{
			Position data = working_buffer.begin();

			if (block_pos + len < block_size)
			{
				memcpy(&memory[block_pos], data, len);
				block_pos += len;
			}
			else
			{
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

		out.position() = pos;
		out.next();
		working_buffer = out.buffer();
	}

public:
	HashingWriteBuffer(
		WriteBuffer & out_,
		size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(block_size_), out(out_), block_size(block_size_), block_pos(0)
	{
		out.next(); /// Если до нас в out что-то уже писали, не дадим остаткам этих данных повлиять на хеш.
		working_buffer = out.buffer();
		pos = working_buffer.begin();
		state = uint128(0, 0);
	}

	uint128 getHash()
	{
		next();
		if (block_pos)
			return CityHash128WithSeed(&memory[0], block_pos, state);
		else
			return state;
	}
};

}
