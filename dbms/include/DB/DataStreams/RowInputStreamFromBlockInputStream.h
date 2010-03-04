#ifndef DBMS_DATA_STREAMS_ROWINPUTSTREAMFROMBLOCKINPUTSTREAM_H
#define DBMS_DATA_STREAMS_ROWINPUTSTREAMFROMBLOCKINPUTSTREAM_H

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


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

#endif
