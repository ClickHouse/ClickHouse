#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для вывода данных в формате tsv.
  */
class TabSeparatedRowOutputStream : public IRowOutputStream
{
public:
	TabSeparatedRowOutputStream(WriteBuffer & ostr_, const Block & sample_);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowEndDelimiter();

	RowOutputStreamPtr clone() { return new TabSeparatedRowOutputStream(ostr, sample); }

private:
	WriteBuffer & ostr;
	const Block & sample;
	DataTypes data_types;
	size_t field_number;
};

}

