#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для ввода данных в формате tsv.
  */
class TabSeparatedRowInputStream : public IRowInputStream
{
public:
	TabSeparatedRowInputStream(ReadBuffer & istr_, const Block & sample_);

	Row read();

	RowInputStreamPtr clone() { return new TabSeparatedRowInputStream(istr, sample); }

private:
	ReadBuffer & istr;
	const Block & sample;
	DataTypes data_types;
};

}
