#pragma once

#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для вывода данных в формате tsv.
  */
class TabSeparatedRowOutputStream : public IRowOutputStream
{
public:
	TabSeparatedRowOutputStream(WriteBuffer & ostr_, SharedPtr<DataTypes> data_types_);

	void writeField(const Field & field);
	void writeFieldDelimiter();
	void writeRowEndDelimiter();

private:
	WriteBuffer & ostr;
	SharedPtr<DataTypes> data_types;
	size_t field_number;
};

}

