#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{


/// Throws exception on encountering prohibited column in block
class ProhibitColumnsBlockOutputStream : public IBlockOutputStream
{
public:
	ProhibitColumnsBlockOutputStream(const BlockOutputStreamPtr & output, const NamesAndTypesList & columns)
		: output{output}, columns{columns}
	{
	}

private:
	void write(const Block & block) override
	{
		for (const auto & column : columns)
			if (block.has(column.name))
				throw Exception{"Cannot insert column " + column.name, ErrorCodes::ILLEGAL_COLUMN};

		output->write(block);
	}

	void flush() override { output->flush(); }

	void writePrefix() override { output->writePrefix(); }
	void writeSuffix() override { output->writeSuffix(); }

	BlockOutputStreamPtr output;
	NamesAndTypesList columns;
};


}
