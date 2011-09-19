#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Defines.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Преобразует поток для чтения данных по строкам в поток для чтения данных по блокам.
  * Наример, для чтения текстового дампа.
  */
class BlockInputStreamFromRowInputStream : public IProfilingBlockInputStream
{
public:
	/** sample_ - пустой блок, который описывает, как интерпретировать значения */
	BlockInputStreamFromRowInputStream(
		IRowInputStream & row_input_,
		const Block & sample_,
		size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

	Block readImpl();

	String getName() const { return "BlockInputStreamFromRowInputStream"; }

private:
	IRowInputStream & row_input;
	const Block & sample;
	size_t max_block_size;
};

}
