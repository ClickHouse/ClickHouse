#ifndef DBMS_COMMON_COMPRESSED_WRITEBUFFER_H
#define DBMS_COMMON_COMPRESSED_WRITEBUFFER_H

#include <vector>

#include <city.h>
#include <quicklz/quicklz_level1.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedWriteBuffer : public WriteBuffer
{
private:
	WriteBuffer & out;

	std::vector<char> compressed_buffer;
	char scratch[QLZ_SCRATCH_COMPRESS];

	size_t compressed_bytes;

	void nextImpl()
	{
		size_t uncompressed_size = pos - working_buffer.begin();
		compressed_buffer.resize(uncompressed_size + QUICKLZ_ADDITIONAL_SPACE);

		size_t compressed_size = qlz_compress(
			working_buffer.begin(),
			&compressed_buffer[0],
			uncompressed_size,
			scratch);

		uint128 checksum = CityHash128(&compressed_buffer[0], compressed_size);
		out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

		out.write(&compressed_buffer[0], compressed_size);
		compressed_bytes += compressed_size;
	}

public:
	CompressedWriteBuffer(WriteBuffer & out_) : out(out_), compressed_bytes(0) {}

	/// Объём сжатых данных
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
