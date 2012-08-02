#pragma once

#include <DB/DataTypes/DataTypeFactory.h>

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Позволяет создать IBlockInputStream или IBlockOutputStream по названию формата.
  * Замечание: формат и сжатие - независимые вещи.
  */
class FormatFactory
{
public:
	BlockInputStreamPtr getInput(const String & name, ReadBuffer & buf,
		Block & sample, size_t max_block_size, const DataTypeFactory & data_type_factory) const;
		
	BlockOutputStreamPtr getOutput(const String & name, WriteBuffer & buf,
		Block & sample) const;
};

}
