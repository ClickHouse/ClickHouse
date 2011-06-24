#ifndef DBMS_COMMON_COMPRESSED_READBUFFER_H
#define DBMS_COMMON_COMPRESSED_READBUFFER_H

#include <vector>

#include <city.h>
#include <quicklz/quicklz_level1.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/CompressedStream.h>


#define DBMS_COMPRESSED_READ_BUFFER_MAX_COMPRESSED_SIZE 0x40000000ULL	/// 1GB


namespace DB
{

class CompressedReadBuffer : public ReadBuffer
{
private:
	ReadBuffer & in;

	std::vector<char> compressed_buffer;
	char scratch[QLZ_SCRATCH_DECOMPRESS];

public:
	CompressedReadBuffer(ReadBuffer & in_)
		: in(in_),
		compressed_buffer(QUICKLZ_HEADER_SIZE)
	{
	}

	bool nextImpl()
	{
		if (in.eof())
			return false;

		uint128 checksum;
		in.readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));
		
		in.readStrict(&compressed_buffer[0], QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(&compressed_buffer[0]);
		if (size_compressed > DBMS_COMPRESSED_READ_BUFFER_MAX_COMPRESSED_SIZE)
			throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);
			
		size_t size_decompressed = qlz_size_decompressed(&compressed_buffer[0]);

		compressed_buffer.resize(size_compressed);
		internal_buffer.resize(size_decompressed);

		in.readStrict(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);

		if (checksum != CityHash128(&compressed_buffer[0], size_compressed))
			throw Exception("Checksum doesnt match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);
				
		qlz_decompress(&compressed_buffer[0], &internal_buffer[0], scratch);

		working_buffer = Buffer(working_buffer.begin(), working_buffer.begin() + size_decompressed);

		return true;
	}
};

}

#endif
