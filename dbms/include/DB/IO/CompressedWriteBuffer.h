#pragma once

#include <math.h>

#include <vector>

#include <city.h>
#include <quicklz/quicklz_level1.h>
#include <lz4/lz4.h>
#include <lz4/lz4hc.h>
#include <zstd/zstd.h>

#include <DB/Common/PODArray.h>
#include <DB/Core/Types.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
	WriteBuffer & out;
	CompressionMethod method;

	PODArray<char> compressed_buffer;
	qlz_state_compress * qlz_state;

	void nextImpl()
	{
		if (!offset())
			return;

		size_t uncompressed_size = offset();
		size_t compressed_size = 0;
		char * compressed_buffer_ptr = nullptr;

		/** Формат сжатого блока - см. CompressedStream.h
		  */

		switch (method)
		{
			case CompressionMethod::QuickLZ:
			{
				compressed_buffer.resize(uncompressed_size + QUICKLZ_ADDITIONAL_SPACE);

				compressed_size = qlz_compress(
					working_buffer.begin(),
					&compressed_buffer[0],
					uncompressed_size,
					qlz_state);

				compressed_buffer[0] &= 3;
				compressed_buffer_ptr = &compressed_buffer[0];
				break;
			}
			case CompressionMethod::LZ4:
			case CompressionMethod::LZ4HC:
			{
				static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

				compressed_buffer.resize(header_size + LZ4_COMPRESSBOUND(uncompressed_size));

				compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::LZ4);

				if (method == CompressionMethod::LZ4)
					compressed_size = header_size + LZ4_compress(
						working_buffer.begin(),
						&compressed_buffer[header_size],
						uncompressed_size);
				else
					compressed_size = header_size + LZ4_compressHC(
						working_buffer.begin(),
						&compressed_buffer[header_size],
						uncompressed_size);

				UInt32 compressed_size_32 = compressed_size;
				UInt32 uncompressed_size_32 = uncompressed_size;

				memcpy(&compressed_buffer[1], reinterpret_cast<const char *>(&compressed_size_32), sizeof(compressed_size_32));
				memcpy(&compressed_buffer[5], reinterpret_cast<const char *>(&uncompressed_size_32), sizeof(uncompressed_size_32));

				compressed_buffer_ptr = &compressed_buffer[0];
				break;
			}
			case CompressionMethod::ZSTD:
			{
				static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

				compressed_buffer.resize(header_size + ZSTD_compressBound(uncompressed_size));

				compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::ZSTD);

				size_t res = ZSTD_compress(
					&compressed_buffer[header_size],
					compressed_buffer.size(),
					working_buffer.begin(),
					uncompressed_size);

				if (ZSTD_isError(res))
					throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

				compressed_size = header_size + res;

				UInt32 compressed_size_32 = compressed_size;
				UInt32 uncompressed_size_32 = uncompressed_size;

				memcpy(&compressed_buffer[1], reinterpret_cast<const char *>(&compressed_size_32), sizeof(compressed_size_32));
				memcpy(&compressed_buffer[5], reinterpret_cast<const char *>(&uncompressed_size_32), sizeof(uncompressed_size_32));

				compressed_buffer_ptr = &compressed_buffer[0];
				break;
			}
			default:
				throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		}

		uint128 checksum = CityHash128(compressed_buffer_ptr, compressed_size);
		out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

		out.write(compressed_buffer_ptr, compressed_size);
	}

public:
	CompressedWriteBuffer(
		WriteBuffer & out_,
		CompressionMethod method_ = CompressionMethod::LZ4,
		size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), method(method_), qlz_state(new qlz_state_compress) {}

	/// Объём сжатых данных
	size_t getCompressedBytes()
	{
		nextIfAtEnd();
		return out.count();
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
		return offset();
	}

	~CompressedWriteBuffer()
	{
		try
		{
			next();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		delete qlz_state;
	}
};

}
