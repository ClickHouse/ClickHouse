#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{


/// Прогресс выполнения запроса
struct Progress
{
	size_t rows;	/// Строк обработано.
	size_t bytes;	/// Байт обработано.

	Progress() : rows(0), bytes(0) {}
	Progress(size_t rows_, size_t bytes_) : rows(rows_), bytes(bytes_) {}

	void read(ReadBuffer & in)
	{
		readBinary(rows, in);
		readBinary(bytes, in);
	}

	void write(WriteBuffer & out)
	{
		writeBinary(rows, out);
		writeBinary(bytes, out);
	}
};


}
