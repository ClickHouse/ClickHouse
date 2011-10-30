#pragma once

#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для чтения данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
	ValuesRowInputStream(ReadBuffer & istr_, SharedPtr<DataTypes> & data_types_);

	Row read();

	RowInputStreamPtr clone() { return new ValuesRowInputStream(istr, data_types); }

private:
	ReadBuffer & istr;
	SharedPtr<DataTypes> data_types;
};

}
