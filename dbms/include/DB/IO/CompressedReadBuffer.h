#pragma once

#include <vector>

#include <city.h>
#include <quicklz/quicklz_level1.h>
#include <lz4/lz4.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
private:
	ReadBuffer & in;

	std::vector<char> compressed_buffer;
	char scratch[QLZ_SCRATCH_DECOMPRESS];

	bool nextImpl()
	{
		if (in.eof())
			return false;

		uint128 checksum;
		in.readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

		in.readStrict(&compressed_buffer[0], QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(&compressed_buffer[0]);
		if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
			throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		size_t size_decompressed = qlz_size_decompressed(&compressed_buffer[0]);

		compressed_buffer.resize(size_compressed);
		memory.resize(size_decompressed);
		internal_buffer.resize(size_decompressed);
		working_buffer.resize(size_decompressed);

		in.readStrict(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);

		if (checksum != CityHash128(&compressed_buffer[0], size_compressed))
			throw Exception("Checksum doesnt match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);

		/// Старший бит первого байта определяет использованный метод сжатия.
		if ((compressed_buffer[0] & 0x80) == 0)
			qlz_decompress(&compressed_buffer[0], working_buffer.begin(), scratch);
		else
			LZ4_uncompress(&compressed_buffer[QUICKLZ_HEADER_SIZE], working_buffer.begin(), size_decompressed);

		return true;
	}

public:
	CompressedReadBuffer(ReadBuffer & in_)
		: in(in_),
		compressed_buffer(QUICKLZ_HEADER_SIZE)
	{
	}
};

}
