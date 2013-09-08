#pragma once

#include <vector>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/UncompressedCache.h>


namespace DB
{

/** Буфер для чтения из сжатого файла с использованием кэша разжатых блоков.
  * Кэш внешний - передаётся в качестве аргумента в конструктор.
  * Позволяет увеличить производительность в случае, когда часто читаются одни и те же блоки.
  * Недостатки:
  * - в случае, если нужно читать много данных подряд, но из них только часть закэширована, приходится делать seek-и.
  */
class CachedCompressedReadBuffer : public ReadBuffer
{
private:
	const std::string path;
	size_t cur_offset; /// Смещение в сжатом файле.
	UncompressedCache & cache;
	size_t buf_size;

	/// SharedPtr - для ленивой инициализации (только в случае кэш-промаха).
	Poco::SharedPtr<ReadBufferFromFile> in;
	Poco::SharedPtr<CompressedReadBuffer> compressed_in;

	/// Кусок данных из кэша, или кусок считанных данных, который мы положим в кэш.
	UncompressedCache::CellPtr owned_cell;

	/// Нужно ли делать seek - при кэш-промахе после кэш-попадания.
	bool need_seek;


	bool nextImpl()
	{
		/// Проверим наличие разжатого блока в кэше, захватим владение этим блоком, если он есть.

		UInt128 key = cache.hash(path, cur_offset);
		owned_cell = cache.get(key);

		if (!owned_cell)
		{
			/// Если нет - надо прочитать его из файла.
			if (!compressed_in)
			{
				in = new ReadBufferFromFile(path, buf_size);
				compressed_in = new CompressedReadBuffer(*in);
			}
			
			if (need_seek)
			{
				in->seek(cur_offset);
				need_seek = false;
			}

			owned_cell = new UncompressedCache::Cell;
			owned_cell->key = key;

			/// Разжимать будем в кусок памяти, который будет в кэше.
			compressed_in->setMemory(owned_cell->data);

			size_t old_count = in->count();
			compressed_in->next();
			owned_cell->compressed_size = in->count() - old_count;

			/// Положим данные в кэш.
			cache.set(owned_cell);
		}
		else
		{
			need_seek = true;
		}

		if (owned_cell->data.m_size == 0)
			return false;

		internal_buffer = Buffer(owned_cell->data.m_data, owned_cell->data.m_data + owned_cell->data.m_size);
		working_buffer = Buffer(owned_cell->data.m_data, owned_cell->data.m_data + owned_cell->data.m_size);
		pos = working_buffer.begin();

		cur_offset += owned_cell->compressed_size;
		return true;
	}

public:
	CachedCompressedReadBuffer(const std::string & path_, size_t offset_, UncompressedCache & cache_, size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE)
		: ReadBuffer(NULL, 0), path(path_), cur_offset(offset_), cache(cache_), buf_size(buf_size_), need_seek(cur_offset != 0)
	{
	}
};

}
