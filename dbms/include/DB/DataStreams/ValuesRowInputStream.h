#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для чтения данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
	ValuesRowInputStream(ReadBuffer & istr_, const Block & sample_);

	Row read();

	RowInputStreamPtr clone() { return new ValuesRowInputStream(istr, sample); }

private:
	ReadBuffer & istr;
	const Block & sample;
	DataTypes data_types;
};

}
