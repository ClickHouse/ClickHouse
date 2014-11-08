#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Поток для чтения данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
	ValuesRowInputStream(ReadBuffer & istr_, const Block & sample_);

	bool read(Row & row) override;

private:
	ReadBuffer & istr;
	const Block sample;
	DataTypes data_types;
};

}
