#include <DB/DataStreams/BinaryRowOutputStream.h>


namespace DB
{


BinaryRowOutputStream::BinaryRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: ostr(ostr_), sample(sample_), field_number(0)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


void BinaryRowOutputStream::writeField(const Field & field)
{
	data_types[field_number]->serializeBinary(field, ostr);
	++field_number;
}


void BinaryRowOutputStream::writeRowEndDelimiter()
{
	field_number = 0;
}

}
