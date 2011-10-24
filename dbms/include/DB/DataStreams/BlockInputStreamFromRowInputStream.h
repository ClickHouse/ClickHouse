#pragma once

#include <DB/Core/Defines.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

/** Преобразует поток для чтения данных по строкам в поток для чтения данных по блокам.
  * Наример, для чтения текстового дампа.
  */
class BlockInputStreamFromRowInputStream : public IProfilingBlockInputStream
{
public:
	/** sample_ - пустой блок, который описывает, как интерпретировать значения */
	BlockInputStreamFromRowInputStream(
		RowInputStreamPtr row_input_,
		const Block & sample_,
		size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

	Block readImpl();

	String getName() const { return "BlockInputStreamFromRowInputStream"; }

	BlockInputStreamPtr clone() { return new BlockInputStreamFromRowInputStream(row_input, sample, max_block_size); }

private:
	RowInputStreamPtr row_input;
	const Block & sample;
	size_t max_block_size;
};

}
