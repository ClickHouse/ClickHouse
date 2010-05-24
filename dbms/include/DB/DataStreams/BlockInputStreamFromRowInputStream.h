#ifndef DBMS_DATA_STREAMS_BLOCKINPUTSTREAMFROMROWINPUTSTREAM_H
#define DBMS_DATA_STREAMS_BLOCKINPUTSTREAMFROMROWINPUTSTREAM_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Defines.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Преобразует поток для чтения данных по строкам в поток для чтения данных по блокам.
  * Наример, для чтения текстового дампа.
  */
class BlockInputStreamFromRowInputStream : public IBlockInputStream
{
public:
	/** sample_ - пустой блок, который описывает, как интерпретировать значения */
	BlockInputStreamFromRowInputStream(
		IRowInputStream & row_input_,
		const Block & sample_,
		size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

	Block read();

private:

	IRowInputStream & row_input;
	const Block & sample;
	size_t max_block_size;

	void initBlock(Block & res);
};

}

#endif
