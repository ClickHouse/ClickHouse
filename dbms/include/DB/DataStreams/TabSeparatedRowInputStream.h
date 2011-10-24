#pragma once

#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для ввода данных в формате tsv.
  */
class TabSeparatedRowInputStream : public IRowInputStream
{
public:
	TabSeparatedRowInputStream(ReadBuffer & istr_, SharedPtr<DataTypes> & data_types_);

	Row read();

	RowInputStreamPtr clone() { return new TabSeparatedRowInputStream(istr, data_types); }

private:
	ReadBuffer & istr;
	SharedPtr<DataTypes> data_types;
};

}
