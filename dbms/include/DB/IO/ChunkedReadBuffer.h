#pragma once

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <iostream>


namespace DB
{


/** Считывает данные, из формата, состоящего из чанков
  * (идентификатор запроса, признак последнего чанка, размер чанка, часть данных со сжатием или без).
  */
class ChunkedReadBuffer : public ReadBuffer
{
protected:
	ReadBuffer & in;
	bool all_read;
	size_t read_in_chunk;
	size_t chunk_size;
	UInt64 assert_query_id;

	bool nextImpl() override
	{
		/// Если прочитали ещё не весь блок - получим следующие данные. Если следующих данных нет - ошибка.
		if (read_in_chunk < chunk_size)
		{
			if (!in.next())
				throw Exception("Cannot read all data from chunked input", ErrorCodes::CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT);

			working_buffer = in.buffer();
			if (chunk_size - read_in_chunk < working_buffer.size())
			{
				working_buffer.resize(chunk_size - read_in_chunk);
				read_in_chunk = chunk_size;
			}
			else
				read_in_chunk += working_buffer.size();

			in.position() += working_buffer.size();
		}
		else
		{
			if (all_read)
				return false;

			UInt64 query_id = 0;
			readIntBinary(query_id, in);

			if (query_id != assert_query_id)
				throw Exception("Received data for wrong query id (expected "
					+ toString(assert_query_id) + ", got "
					+ toString(query_id) + ")", ErrorCodes::RECEIVED_DATA_FOR_WRONG_QUERY_ID);

			/// Флаг конца.
			readIntBinary(all_read, in);
			/// Размер блока.
			readIntBinary(chunk_size, in);

			read_in_chunk = std::min(chunk_size, in.buffer().size() - in.offset());
			working_buffer = Buffer(in.position(), in.position() + read_in_chunk);
			in.position() += read_in_chunk;

			if (all_read)
				return false;
		}

		return true;
	}

public:
	ChunkedReadBuffer(ReadBuffer & in_, UInt64 assert_query_id_)
		: ReadBuffer(nullptr, 0), in(in_), all_read(false), read_in_chunk(0), chunk_size(0), assert_query_id(assert_query_id_) {}
};


}
