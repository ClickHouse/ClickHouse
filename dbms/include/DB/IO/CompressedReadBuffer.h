#ifndef DBMS_COMMON_COMPRESSED_READBUFFER_H
#define DBMS_COMMON_COMPRESSED_READBUFFER_H

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
	char scratch[QLZ_SCRATCH_COMPRESS];

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
		size_t size = in.read(&compressed_buffer[0], QUICKLZ_HEADER_SIZE);
		if (size != QUICKLZ_HEADER_SIZE)
			throw Exception("Cannot read size of compressed chunk", ErrorCodes::CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK);

		size_t size_compressed = qlz_size_compressed(internal_buffer);
		size_t size_decompressed = qlz_size_decompressed(internal_buffer);

		compressed_buffer.resize(size_compressed);
		decompressed_buffer.resize(size_decompressed);
		
		size = in.read(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);
		if (size != size_compressed - QUICKLZ_HEADER_SIZE)
			throw Exception("Cannot read compressed chunk", ErrorCodes::CANNOT_READ_COMPRESSED_CHUNK);

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
	
		size_t bytes_to_copy = std::min(decompressed_buffer.size() - pos_in_buffer, DEFAULT_READ_BUFFER_SIZE);
		std::memcpy(internal_buffer, &decompressed_buffer[pos_in_buffer], bytes_to_copy);
	
		pos = internal_buffer;
		working_buffer = Buffer(internal_buffer, internal_buffer + bytes_to_copy);

		return true;
	}
};

}

#endif
