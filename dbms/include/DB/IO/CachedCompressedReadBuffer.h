#pragma once

#include <DB/IO/createReadBufferFromFileBase.h>
#include <DB/IO/CompressedReadBufferBase.h>
#include <DB/IO/UncompressedCache.h>


namespace DB
{

/** Буфер для чтения из сжатого файла с использованием кэша разжатых блоков.
  * Кэш внешний - передаётся в качестве аргумента в конструктор.
  * Позволяет увеличить производительность в случае, когда часто читаются одни и те же блоки.
  * Недостатки:
  * - в случае, если нужно читать много данных подряд, но из них только часть закэширована, приходится делать seek-и.
  */
class CachedCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
private:
	const std::string path;
	UncompressedCache * cache;
	size_t buf_size;
	size_t estimated_size;
	size_t aio_threshold;

	/// SharedPtr - для ленивой инициализации (только в случае кэш-промаха).
	Poco::SharedPtr<ReadBufferFromFileBase> file_in;
	size_t file_pos;

	/// Кусок данных из кэша, или кусок считанных данных, который мы положим в кэш.
	UncompressedCache::MappedPtr owned_cell;

	void initInput()
	{
		if (!file_in)
		{
			file_in = createReadBufferFromFileBase(path, estimated_size, aio_threshold, buf_size);
			compressed_in = &*file_in;
		}
	}

	bool nextImpl()
	{
		/// Проверим наличие разжатого блока в кэше, захватим владение этим блоком, если он есть.

		UInt128 key = cache->hash(path, file_pos);
		owned_cell = cache->get(key);

		if (!owned_cell)
		{
			/// Если нет - надо прочитать его из файла.
			initInput();
			file_in->seek(file_pos);

			owned_cell.reset(new UncompressedCacheCell);

			size_t size_decompressed;
			size_t size_compressed_without_checksum;
			owned_cell->compressed_size = readCompressedData(size_decompressed, size_compressed_without_checksum);

			if (owned_cell->compressed_size)
			{
				owned_cell->data.resize(size_decompressed);
				decompress(owned_cell->data.m_data, size_decompressed, size_compressed_without_checksum);

				/// Положим данные в кэш.
				cache->set(key, owned_cell);
			}
		}

		if (owned_cell->data.m_size == 0)
		{
			owned_cell = nullptr;
			return false;
		}

		working_buffer = Buffer(owned_cell->data.m_data, owned_cell->data.m_data + owned_cell->data.m_size);

		file_pos += owned_cell->compressed_size;

		return true;
	}

public:
	CachedCompressedReadBuffer(const std::string & path_, UncompressedCache * cache_, size_t estimated_size_,
							   size_t aio_threshold_, size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE)
		: ReadBuffer(nullptr, 0), path(path_), cache(cache_), buf_size(buf_size_),
		estimated_size(estimated_size_), aio_threshold(aio_threshold_), file_pos(0)
	{
	}


	void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
	{
		if (owned_cell &&
			offset_in_compressed_file == file_pos - owned_cell->compressed_size &&
			offset_in_decompressed_block <= working_buffer.size())
		{
			bytes += offset();
			pos = working_buffer.begin() + offset_in_decompressed_block;
			bytes -= offset();
		}
		else
		{
			file_pos = offset_in_compressed_file;

			bytes += offset();
			nextImpl();

			if (offset_in_decompressed_block > working_buffer.size())
				throw Exception("Seek position is beyond the decompressed block", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

			pos = working_buffer.begin() + offset_in_decompressed_block;
			bytes -= offset();
		}
	}
};

}
