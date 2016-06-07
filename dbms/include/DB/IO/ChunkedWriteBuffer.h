#pragma once

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>

#include <iostream>


namespace DB
{

	
/** Записывает данные в формат, состоящий из чанков
  * (идентификатор запроса, признак последнего чанка, размер чанка, часть данных со сжатием или без).
  * Нельзя использовать out напрямую.
  */
class ChunkedWriteBuffer : public WriteBuffer
{
protected:
	WriteBuffer & out;
	UInt64 query_id;

	inline size_t headerSize() { return sizeof(query_id) + sizeof(bool) + sizeof(size_t); }

	void checkBufferSize()
	{
		if (out.buffer().end() - out.position() < 2 * static_cast<int>(headerSize()))
			throw Exception("Too small remaining buffer size to write chunked data", ErrorCodes::TOO_SMALL_BUFFER_SIZE);
	}

	void nextImpl()
	{
/*		std::cerr << out.offset() << std::endl;
		std::cerr << query_id << std::endl;
		std::cerr << offset() << std::endl;*/
		
		checkBufferSize();

		writeIntBinary(query_id, out);
		writeIntBinary(false, out);
		writeIntBinary(offset(), out);

//		std::cerr << out.offset() << std::endl;

		out.position() = position();
		out.next();
		working_buffer = Buffer(out.buffer().begin() + headerSize(), out.buffer().end());
		pos = working_buffer.begin();
	}

public:
	ChunkedWriteBuffer(WriteBuffer & out_, UInt64 query_id_)
		: WriteBuffer(out_.position() + headerSize(), out_.buffer().size() - out_.offset() - headerSize()), out(out_), query_id(query_id_)
	{
		checkBufferSize();
	}

	void finish()
	{
		next();

		writeIntBinary(query_id, out);
		writeIntBinary(true, out);
		writeIntBinary(static_cast<size_t>(0), out);
	}
};


}
