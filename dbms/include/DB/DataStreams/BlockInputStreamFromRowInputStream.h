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
		size_t max_block_size_ = DEFAULT_INSERT_BLOCK_SIZE);	/// Обычно дамп читается в целях вставки в таблицу.

	void readPrefix() override { row_input->readPrefix(); }
	void readSuffix() override { row_input->readSuffix(); }

	String getName() const override { return "BlockInputStreamFromRowInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

	RowInputStreamPtr & getRowInput() { return row_input; }

protected:
	Block readImpl() override;

private:
	RowInputStreamPtr row_input;
	const Block sample;
	size_t max_block_size;
	size_t total_rows;
};

}
