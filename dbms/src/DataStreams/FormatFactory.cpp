#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/ValuesRowInputStream.h>
#include <DB/DataStreams/ValuesRowOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/FormatFactory.h>


namespace DB
{

BlockInputStreamPtr FormatFactory::getInput(const String & name, ReadBuffer & buf,
	Block & sample, size_t max_block_size, DataTypeFactory & data_type_factory) const
{
	if (name == "Native")
		return new NativeBlockInputStream(buf, data_type_factory);
	else if (name == "TabSeparated")
		return new BlockInputStreamFromRowInputStream(new TabSeparatedRowInputStream(buf, sample), sample, max_block_size);
	else if (name == "Values")
		return new BlockInputStreamFromRowInputStream(new ValuesRowInputStream(buf, sample), sample, max_block_size);
	else
		throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}


BlockOutputStreamPtr FormatFactory::getOutput(const String & name, WriteBuffer & buf,
	Block & sample) const
{
	if (name == "Native")
		return new NativeBlockOutputStream(buf);
	else if (name == "TabSeparated")
		return new BlockOutputStreamFromRowOutputStream(new TabSeparatedRowOutputStream(buf, sample));
	else if (name == "Values")
		return new BlockOutputStreamFromRowOutputStream(new ValuesRowOutputStream(buf, sample));
	else
		throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

}
