#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_COLUMN;
}


/// Throws exception on encountering prohibited column in block
class ProhibitColumnsBlockOutputStream : public IBlockOutputStream
{
public:
	ProhibitColumnsBlockOutputStream(const BlockOutputStreamPtr & output, const NamesAndTypesList & columns)
		: output{output}, columns{columns}
	{
	}

private:
	void write(const Block & block) override;

	void flush() override { output->flush(); }

	void writePrefix() override { output->writePrefix(); }
	void writeSuffix() override { output->writeSuffix(); }

	BlockOutputStreamPtr output;
	NamesAndTypesList columns;
};


}
