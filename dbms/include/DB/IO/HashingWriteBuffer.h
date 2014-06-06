#pragma once

#include <city.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/ReadHelpers.h>

#define DBMS_DEFAULT_HASHING_BLOCK_SIZE 2048ULL


namespace DB
{

template <class Buffer>
class IHashingBuffer : public BufferWithOwnMemory<Buffer>
{
public:
	IHashingBuffer<Buffer>(size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE) :
		block_pos(0), block_size(block_size_), state(0, 0)
	{
	}

	uint128 getHash()
	{
		if (block_pos)
			return CityHash128WithSeed(&BufferWithOwnMemory<Buffer>::memory[0], block_pos, state);
		else
			return state;
	}

	void append(DB::BufferBase::Position data)
	{
		state = CityHash128WithSeed(data, block_size, state);
	}

	/// вычисление хэша зависит от разбиения по блокам
	/// поэтому нужно вычислить хэш от n полных кусочков и одного неполного
	void calculateHash(DB::BufferBase::Position data, size_t len)
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

protected:
	size_t block_pos;
	size_t block_size;
	uint128 state;
};

/** Вычисляет хеш от записываемых данных и передает их в указанный WriteBuffer.
  * В качестве основного буфера используется буфер вложенного WriteBuffer.
  */
class HashingWriteBuffer : public IHashingBuffer<WriteBuffer>
{
private:
	WriteBuffer & out;

	void nextImpl() override
	{
		size_t len = offset();

		Position data = working_buffer.begin();
		calculateHash(data, len);

		out.position() = pos;
		out.next();
		working_buffer = out.buffer();
	}

public:
	HashingWriteBuffer(
		WriteBuffer & out_,
		size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
		: IHashingBuffer<DB::WriteBuffer>(block_size_), out(out_)
	{
		out.next(); /// Если до нас в out что-то уже писали, не дадим остаткам этих данных повлиять на хеш.
		working_buffer = out.buffer();
		pos = working_buffer.begin();
		state = uint128(0, 0);
	}

	uint128 getHash()
	{
		next();
		return IHashingBuffer<WriteBuffer>::getHash();
	}
};
}


std::string uint128ToString(uint128 data);

std::ostream & operator<<(std::ostream & os, const uint128 & data);
std::istream & operator>>(std::istream & is, uint128 & data);
