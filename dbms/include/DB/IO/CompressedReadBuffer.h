#ifndef DBMS_COMMON_COMPRESSED_READBUFFER_H
#define DBMS_COMMON_COMPRESSED_READBUFFER_H

#include <vector>
#include <algorithm>

#include <quicklz/quicklz_level1.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/CompressedStream.h>


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

	bool next()
	{
		if (in.eof())
			return false;
		
		in.readStrict(&compressed_buffer[0], QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(&compressed_buffer[0]);
		size_t size_decompressed = qlz_size_decompressed(&compressed_buffer[0]);

		compressed_buffer.resize(size_compressed);
		internal_buffer.resize(size_decompressed);

		in.readStrict(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);

		qlz_decompress(&compressed_buffer[0], &internal_buffer[0], scratch);

		pos = working_buffer.begin();
		working_buffer = Buffer(working_buffer.begin(), working_buffer.begin() + size_decompressed);

		return true;
	}
};

}

#endif
