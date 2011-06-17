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
	std::vector<char> decompressed_buffer;
	char scratch[QLZ_SCRATCH_DECOMPRESS];

	size_t pos_in_buffer;

public:
	CompressedReadBuffer(ReadBuffer & in_)
		: in(in_),
		compressed_buffer(QUICKLZ_HEADER_SIZE),
		pos_in_buffer(0)
	{
	}

	/** Читает и разжимает следующий кусок сжатых данных. */
	void readCompressedChunk()
	{
		in.readStrict(&compressed_buffer[0], QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(&compressed_buffer[0]);
		size_t size_decompressed = qlz_size_decompressed(&compressed_buffer[0]);

		compressed_buffer.resize(size_compressed);
		decompressed_buffer.resize(size_decompressed);

		in.readStrict(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);
		
		qlz_decompress(&compressed_buffer[0], &decompressed_buffer[0], scratch);

		pos_in_buffer = 0;
	}

	bool next()
	{
		if (pos_in_buffer == decompressed_buffer.size())
		{
			if (in.eof())
				return false;

			readCompressedChunk();
		}
	
		size_t bytes_to_copy = std::min(decompressed_buffer.size() - pos_in_buffer,
			static_cast<size_t>(DEFAULT_READ_BUFFER_SIZE));
		std::memcpy(working_buffer.begin(), &decompressed_buffer[pos_in_buffer], bytes_to_copy);

		pos_in_buffer += bytes_to_copy;
		pos = working_buffer.begin();
		working_buffer = Buffer(working_buffer.begin(), working_buffer.begin() + bytes_to_copy);

		return true;
	}
};

}

#endif
