#pragma once

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{


/** Преобразует поток для чтения данных по блокам в поток для чтения данных по строкам.
  */
class RowInputStreamFromBlockInputStream : public IRowInputStream
{
public:
	explicit RowInputStreamFromBlockInputStream(IBlockInputStream & block_input_);
	Row read();

private:
	IBlockInputStream & block_input;

	size_t pos;
	size_t current_rows;
	Block current_block;
};

}
