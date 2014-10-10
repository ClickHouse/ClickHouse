#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{


/// Throws exception on encountering prohibited column in block
class ProhibitColumnsBlockOutputStream : public IBlockOutputStream
{
public:
	ProhibitColumnsBlockOutputStream(BlockOutputStreamPtr output, const NamesAndTypesList & columns)
		: output{output}, columns{columns}
	{
	}

	void write(const Block & block) override
	{
        for (const auto & column : columns)
            if (block.has(column.name))
				throw Exception{"Cannot insert column " + column.name, ErrorCodes::ILLEGAL_COLUMN};

		output->write(block);
	}

	void flush() { output->flush(); }

	void writePrefix() override { output->writePrefix(); }
	void writeSuffix() override { output->writeSuffix(); }

private:
	BlockOutputStreamPtr output;
	NamesAndTypesList columns;
};


}
