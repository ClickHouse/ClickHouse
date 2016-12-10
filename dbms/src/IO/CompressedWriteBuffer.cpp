#include <memory>
#include <city.h>

#ifdef USE_QUICKLZ
	#include <quicklz/quicklz_level1.h>
#endif

#include <lz4/lz4.h>
#include <lz4/lz4hc.h>
#include <zstd/zstd.h>

#include <DB/Common/unaligned.h>

#include <DB/IO/CompressedWriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_COMPRESS;
	extern const int UNKNOWN_COMPRESSION_METHOD;
}


void CompressedWriteBuffer::nextImpl()
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
		#ifdef USE_QUICKLZ
			compressed_buffer.resize(uncompressed_size + QUICKLZ_ADDITIONAL_SPACE);

			compressed_size = qlz_compress(
				working_buffer.begin(),
				&compressed_buffer[0],
				uncompressed_size,
				qlz_state.get());

			compressed_buffer[0] &= 3;
			compressed_buffer_ptr = &compressed_buffer[0];
			break;
		#else
			throw Exception("QuickLZ compression method is disabled", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		#endif
		}
		case CompressionMethod::LZ4:
		case CompressionMethod::LZ4HC:
		{
			static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
			compressed_buffer.resize(header_size + LZ4_COMPRESSBOUND(uncompressed_size));
#pragma GCC diagnostic pop

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

			unalignedStore(&compressed_buffer[1], compressed_size_32);
			unalignedStore(&compressed_buffer[5], uncompressed_size_32);

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
				uncompressed_size,
				1);

			if (ZSTD_isError(res))
				throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

			compressed_size = header_size + res;

			UInt32 compressed_size_32 = compressed_size;
			UInt32 uncompressed_size_32 = uncompressed_size;

			unalignedStore(&compressed_buffer[1], compressed_size_32);
			unalignedStore(&compressed_buffer[5], uncompressed_size_32);

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


CompressedWriteBuffer::CompressedWriteBuffer(
	WriteBuffer & out_,
	CompressionMethod method_,
	size_t buf_size)
	: BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), method(method_)
#ifdef USE_QUICKLZ
		, qlz_state(std::make_unique<qlz_state_compress>())
#endif
{
}


CompressedWriteBuffer::~CompressedWriteBuffer()
{
	try
	{
		next();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}

}
