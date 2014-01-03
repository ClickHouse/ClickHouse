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

class CompressedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
private:
	ReadBuffer & in;

	/// Если в буфере in помещается целый сжатый блок - используем его. Иначе - копируем данные по кусочкам в own_compressed_buffer.
	PODArray<char> own_compressed_buffer;
	char * compressed_buffer;
	
	qlz_state_decompress * qlz_state;

	/** Указатель на кусок памяти, куда будут разжиматься блоки.
	  * Это может быть либо свой кусок памяти из BufferWithOwnMemory (по-умолчанию),
	  *  либо пользователь может попросить разжимать данные в свой кусок памяти (метод setMemory).
	  */
	Memory * maybe_own_memory;


	/// Прочитать сжатые данные в compressed_buffer. Достать из их заголовка размер разжатых данных. Проверить чексумму.
	bool readCompressedData(size_t & size_decompressed)
	{
		if (in.eof())
			return false;

		uint128 checksum;
		in.readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

		in.readStrict(&own_compressed_buffer[0], QUICKLZ_HEADER_SIZE);

		size_t size_compressed = qlz_size_compressed(&own_compressed_buffer[0]);
		if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
			throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

		ProfileEvents::increment(ProfileEvents::ReadCompressedBytes, size_compressed + sizeof(checksum));

		size_decompressed = qlz_size_decompressed(&own_compressed_buffer[0]);

		/// Находится ли сжатый блок целиком в буфере in?
		if (in.offset() >= QUICKLZ_HEADER_SIZE && in.position() + size_compressed - QUICKLZ_HEADER_SIZE <= in.buffer().end())
		{
			in.position() -= QUICKLZ_HEADER_SIZE;
			compressed_buffer = in.position();
			in.position() += size_compressed;
		}
		else
		{
			own_compressed_buffer.resize(size_compressed);
			compressed_buffer = &own_compressed_buffer[0];
			in.readStrict(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);
		}

		if (checksum != CityHash128(&compressed_buffer[0], size_compressed))
			throw Exception("Checksum doesn't match: corrupted data.", ErrorCodes::CHECKSUM_DOESNT_MATCH);

		return true;
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
			LZ4_uncompress(&compressed_buffer[QUICKLZ_HEADER_SIZE], to, size_decompressed);
	}

	bool nextImpl()
	{
		size_t size_decompressed = 0;

		if (!readCompressedData(size_decompressed))
			return false;

		maybe_own_memory->resize(size_decompressed);
		internal_buffer = Buffer(&(*maybe_own_memory)[0], &(*maybe_own_memory)[size_decompressed]);
		working_buffer = Buffer(&(*maybe_own_memory)[0], &(*maybe_own_memory)[size_decompressed]);

		decompress(working_buffer.begin(), size_decompressed);

		return true;
	}

public:
	CompressedReadBuffer(ReadBuffer & in_)
		: BufferWithOwnMemory<ReadBuffer>(0),
		in(in_),
		own_compressed_buffer(QUICKLZ_HEADER_SIZE),
		compressed_buffer(NULL),
		qlz_state(NULL),
		maybe_own_memory(&memory)
	{
	}

    ~CompressedReadBuffer()
	{
		if (qlz_state)
			delete qlz_state;
	}


	/// Использовать предоставленный пользователем кусок памяти для разжатия. (Для реализации кэша разжатых блоков.)
	void setMemory(Memory & memory_)
	{
		maybe_own_memory = &memory_;
	}


	size_t readBig(char * to, size_t n)
	{
		size_t bytes_read = 0;

		/// Если в буфере есть непрочитанные байты, то скопируем сколько надо в to.
		if (pos < working_buffer.end())
			bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

		if (bytes_read < n)
			bytes += offset();

		/// Если надо ещё прочитать - будем, по возможности, разжимать сразу в to.
		while (bytes_read < n)
		{
			size_t size_decompressed = 0;

			if (!readCompressedData(size_decompressed))
			{
				working_buffer.resize(0);
				pos = working_buffer.begin();
				return bytes_read;
			}

			/// Если разжатый блок помещается целиком туда, куда его надо скопировать.
			if (size_decompressed <= n - bytes_read)
			{
				decompress(to + bytes_read, size_decompressed);
				bytes_read += size_decompressed;
				bytes += size_decompressed;
			}
			else
			{
				maybe_own_memory->resize(size_decompressed);
				internal_buffer = Buffer(&(*maybe_own_memory)[0], &(*maybe_own_memory)[size_decompressed]);
				working_buffer = Buffer(&(*maybe_own_memory)[0], &(*maybe_own_memory)[size_decompressed]);
				pos = working_buffer.begin();

				decompress(working_buffer.begin(), size_decompressed);

				bytes_read += read(to + bytes_read, n - bytes_read);
				break;
			}
		}

		return bytes_read;
	}
};

}
