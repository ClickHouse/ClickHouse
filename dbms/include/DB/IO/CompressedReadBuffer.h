#ifndef DBMS_COMMON_COMPRESSED_READBUFFER_H
#define DBMS_COMMON_COMPRESSED_READBUFFER_H

#include <vector>
#include <algorithm>

#include <snappy.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/VarInt.h>


/// Если сжатый кусок больше 1GB - значит ошибка
#define DB_COMPRESSED_BUFFER_MAX_COMPRESSED_SIZE 0x40000000


namespace DB
{

class CompressedReadBuffer : public ReadBuffer
{
private:
	ReadBuffer & in;

	std::vector<char> compressed_buffer;

public:
	CompressedReadBuffer(ReadBuffer & in_)
		: in(in_)
	{
	}

	bool next()
	{
		if (in.eof())
			return false;

		size_t size_compressed = 0;
		readVarUInt(size_compressed, in);
		if (size_compressed == 0)
			throw Exception("Too small size_compressed", ErrorCodes::TOO_SMALL_SIZE_COMPRESSED);
		if (size_compressed > DB_COMPRESSED_BUFFER_MAX_COMPRESSED_SIZE)
			throw Exception("Too large size_compressed", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		compressed_buffer.resize(size_compressed);
		in.readStrict(&compressed_buffer[0], size_compressed);

		size_t size_decompressed = 0;
		if (!snappy::GetUncompressedLength(&compressed_buffer[0], size_compressed, &size_decompressed))
			throw Exception("Cannot decompress corrupted data", ErrorCodes::CANNOT_DECOMPRESS_CORRUPTED_DATA);
		
		internal_buffer.resize(size_decompressed);

		if (!snappy::RawUncompress(&compressed_buffer[0], size_compressed, &internal_buffer[0]))
			throw Exception("Cannot decompress corrupted data", ErrorCodes::CANNOT_DECOMPRESS_CORRUPTED_DATA);

		working_buffer = Buffer(working_buffer.begin(), working_buffer.begin() + size_decompressed);
		pos = working_buffer.begin();

		return true;
	}
};

}

#undef DB_COMPRESSED_BUFFER_MAX_COMPRESSED_SIZE

#endif
