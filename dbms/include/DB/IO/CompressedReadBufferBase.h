#pragma once

#include <vector>

#include <city.h>
#include <quicklz/quicklz_level1.h>
#include <lz4/lz4.h>

#include <DB/Common/PODArray.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedReadBufferBase
{
protected:
	ReadBuffer * compressed_in;

	/// Если в буфере compressed_in помещается целый сжатый блок - используем его. Иначе - копируем данные по кусочкам в own_compressed_buffer.
	PODArray<char> own_compressed_buffer;
	char * compressed_buffer;

	qlz_state_decompress * qlz_state;

	/// Прочитать сжатые данные в compressed_buffer. Достать из их заголовка размер разжатых данных. Проверить чексумму.
	/// Возвращает количество прочитанных байт.
	size_t readCompressedData(size_t & size_decompressed)
	{
		if (compressed_in->eof())
			return 0;

		uint128 checksum;
		compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

		own_compressed_buffer.resize(QUICKLZ_HEADER_SIZE);
		compressed_in->readStrict(&own_compressed_buffer[0], QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(&own_compressed_buffer[0]);
		if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
			throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed + sizeof(checksum));

		size_decompressed = qlz_size_decompressed(&own_compressed_buffer[0]);

		/// Находится ли сжатый блок целиком в буфере compressed_in?
		if (compressed_in->offset() >= QUICKLZ_HEADER_SIZE &&
			compressed_in->position() + size_compressed - QUICKLZ_HEADER_SIZE <= compressed_in->buffer().end())
		{
			compressed_in->position() -= QUICKLZ_HEADER_SIZE;
			compressed_buffer = compressed_in->position();
			compressed_in->position() += size_compressed;
		}
		else
		{
			own_compressed_buffer.resize(size_compressed);
			compressed_buffer = &own_compressed_buffer[0];
			compressed_in->readStrict(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);
		}

		if (checksum != CityHash128(&compressed_buffer[0], size_compressed))
			throw Exception("Checksum doesn't match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);

		return size_compressed + sizeof(checksum);
	}

	void decompress(char * to, size_t size_decompressed)
	{
		ProfileEvents::increment(ProfileEvents::CompressedReadBufferBlocks);
		ProfileEvents::increment(ProfileEvents::CompressedReadBufferBytes, size_decompressed);

		/// Старший бит первого байта определяет использованный метод сжатия.
		if ((compressed_buffer[0] & 0x80) == 0)
		{
			if (!qlz_state)
				qlz_state = new qlz_state_decompress;

			qlz_decompress(&compressed_buffer[0], to, qlz_state);
		}
		else
			LZ4_decompress_fast(&compressed_buffer[QUICKLZ_HEADER_SIZE], to, size_decompressed);
	}

public:
	/// compressed_in можно инициализировать отложенно, но до первого вызова readCompressedData.
	CompressedReadBufferBase(ReadBuffer * in = NULL)
		:
		compressed_in(in),
		own_compressed_buffer(QUICKLZ_HEADER_SIZE),
		compressed_buffer(NULL),
		qlz_state(NULL)
	{
	}

	~CompressedReadBufferBase()
	{
		if (qlz_state)
			delete qlz_state;
	}
};

}
