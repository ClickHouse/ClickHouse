#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

/** Поток для ввода данных в бинарном построчном формате.
  */
class BinaryRowInputStream : public IRowInputStream
{
public:
	BinaryRowInputStream(ReadBuffer & istr_, const Block & sample_);

	bool read(Row & row) override;

private:
	ReadBuffer & istr;
	const Block sample;
	DataTypes data_types;
};

}
