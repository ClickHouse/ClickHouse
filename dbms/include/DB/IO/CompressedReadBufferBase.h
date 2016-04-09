#pragma once

#include <vector>

#include <city.h>

#ifdef USE_QUICKLZ
	#include <quicklz/quicklz_level1.h>
#endif

#include <lz4/lz4.h>
#include <zstd/zstd.h>

#include <DB/Common/PODArray.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/Exception.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/CompressedStream.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_COMPRESSION_METHOD;
	extern const int TOO_LARGE_SIZE_COMPRESSED;
	extern const int CHECKSUM_DOESNT_MATCH;
	extern const int CANNOT_DECOMPRESS;
}


class CompressedReadBufferBase
{
protected:
	ReadBuffer * compressed_in;

	/// Если в буфере compressed_in помещается целый сжатый блок - используем его. Иначе - копируем данные по кусочкам в own_compressed_buffer.
	PODArray<char> own_compressed_buffer{COMPRESSED_BLOCK_HEADER_SIZE};
	char * compressed_buffer = nullptr;

#ifdef USE_QUICKLZ
	qlz_state_decompress * qlz_state = nullptr;
#else
	void * fixed_size_padding = nullptr;
#endif

	/// Не проверять чексуммы.
	bool disable_checksum = false;


	/// Прочитать сжатые данные в compressed_buffer. Достать из их заголовка размер разжатых данных. Проверить чексумму.
	/// Возвращает количество прочитанных байт.
	size_t readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
	{
		if (compressed_in->eof())
			return 0;

		uint128 checksum;
		compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

		own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
		compressed_in->readStrict(&own_compressed_buffer[0], COMPRESSED_BLOCK_HEADER_SIZE);

		UInt8 method = own_compressed_buffer[0];	/// См. CompressedWriteBuffer.h

		size_t & size_compressed = size_compressed_without_checksum;

		if (method < 0x80)
		{
		#ifdef USE_QUICKLZ
			size_compressed = qlz_size_compressed(&own_compressed_buffer[0]);
			size_decompressed = qlz_size_decompressed(&own_compressed_buffer[0]);
		#else
			throw Exception("QuickLZ compression method is disabled", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		#endif
		}
		else if (method == static_cast<UInt8>(CompressionMethodByte::LZ4) || method == static_cast<UInt8>(CompressionMethodByte::ZSTD))
		{
			size_compressed = *reinterpret_cast<const UInt32 *>(&own_compressed_buffer[1]);
			size_decompressed = *reinterpret_cast<const UInt32 *>(&own_compressed_buffer[5]);
		}
		else
			throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

		if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
			throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed + sizeof(checksum));

		/// Находится ли сжатый блок целиком в буфере compressed_in?
		if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE &&
			compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
		{
			compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
			compressed_buffer = compressed_in->position();
			compressed_in->position() += size_compressed;
		}
		else
		{
			own_compressed_buffer.resize(size_compressed);
			compressed_buffer = &own_compressed_buffer[0];
			compressed_in->readStrict(&compressed_buffer[COMPRESSED_BLOCK_HEADER_SIZE], size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
		}

		if (!disable_checksum && checksum != CityHash128(&compressed_buffer[0], size_compressed))
			throw Exception("Checksum doesn't match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);

		return size_compressed + sizeof(checksum);
	}

	void decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
	{
		ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
		ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

		UInt8 method = compressed_buffer[0];	/// См. CompressedWriteBuffer.h

		if (method < 0x80)
		{
		#ifdef USE_QUICKLZ
			if (!qlz_state)
				qlz_state = new qlz_state_decompress;

			qlz_decompress(&compressed_buffer[0], to, qlz_state);
		#else
			throw Exception("QuickLZ compression method is disabled", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		#endif
		}
		else if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
		{
			if (LZ4_decompress_fast(&compressed_buffer[COMPRESSED_BLOCK_HEADER_SIZE], to, size_decompressed) < 0)
				throw Exception("Cannot LZ4_decompress_fast", ErrorCodes::CANNOT_DECOMPRESS);
		}
		else if (method == static_cast<UInt8>(CompressionMethodByte::ZSTD))
		{
			size_t res = ZSTD_decompress(
				to, size_decompressed,
				&compressed_buffer[COMPRESSED_BLOCK_HEADER_SIZE], size_compressed_without_checksum - COMPRESSED_BLOCK_HEADER_SIZE);

			if (ZSTD_isError(res))
				throw Exception("Cannot ZSTD_decompress: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_DECOMPRESS);
		}
		else
			throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
	}

public:
	/// compressed_in можно инициализировать отложенно, но до первого вызова readCompressedData.
	CompressedReadBufferBase(ReadBuffer * in = nullptr)
		: compressed_in(in)
	{
	}

	~CompressedReadBufferBase()
	{
	#ifdef USE_QUICKLZ
		if (qlz_state)
			delete qlz_state;
	#endif
	}

	/** Не проверять чексуммы.
	  * Может использоваться, например, в тех случаях, когда сжатые данные пишет клиент,
	  *  который не умеет вычислять чексуммы, и вместо этого заполняет их нулями или чем угодно.
	  */
	void disableChecksumming()
	{
		disable_checksum = true;
	}
};

}
