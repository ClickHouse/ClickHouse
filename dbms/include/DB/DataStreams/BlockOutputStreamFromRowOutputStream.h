#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

/** Преобразует поток для записи данных по строкам в поток для записи данных по блокам.
  * Наример, для записи текстового дампа.
  */
class BlockOutputStreamFromRowOutputStream : public IBlockOutputStream
{
public:
	BlockOutputStreamFromRowOutputStream(RowOutputStreamPtr row_output_);
	void write(const Block & block);
	void writePrefix() { row_output->writePrefix(); }
	void writeSuffix() { row_output->writeSuffix(); }

	BlockOutputStreamPtr clone() { return new BlockOutputStreamFromRowOutputStream(row_output); }

private:
	RowOutputStreamPtr row_output;
};

}
