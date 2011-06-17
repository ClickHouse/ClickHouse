#ifndef DBMS_COMMON_COMPRESSED_WRITEBUFFER_H
#define DBMS_COMMON_COMPRESSED_WRITEBUFFER_H

#include <snappy.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/VarInt.h>


namespace DB
{

class CompressedWriteBuffer : public WriteBuffer
{
private:
	WriteBuffer & out;

	std::vector<char> compressed_buffer;
	size_t compressed_bytes;

public:
	CompressedWriteBuffer(WriteBuffer & out_) : out(out_), compressed_bytes(0) {}

	void next()
	{
		compressed_buffer.resize(snappy::MaxCompressedLength(pos - working_buffer.begin()));
		size_t compressed_size = 0;
		snappy::RawCompress(
			working_buffer.begin(),
			pos - working_buffer.begin(),
			&compressed_buffer[0],
			&compressed_size);

		DB::writeVarUInt(compressed_size, out);
		out.write(&compressed_buffer[0], compressed_size);
		pos = working_buffer.begin();
		compressed_bytes += compressed_size + DB::getLengthOfVarUInt(compressed_size);
	}

	/// Объём данных, которые были сжаты
	size_t getCompressedBytes()
	{
		nextIfAtEnd();
		return compressed_bytes;
	}

	/// Сколько несжатых байт было записано в буфер
	size_t getUncompressedBytes()
	{
		return count();
	}

	/// Сколько байт находится в буфере (ещё не сжато)
	size_t getRemainingBytes()
	{
		nextIfAtEnd();
		return pos - working_buffer.begin();
	}

	~CompressedWriteBuffer()
	{
		next();
	}
};

}

#endif
