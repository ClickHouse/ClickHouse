#pragma once

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

class Context;


/** Поток для чтения данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
	ValuesRowInputStream(ReadBuffer & istr_, const Block & sample_, const Context & context_);

	bool read(Row & row) override;

private:
	ReadBuffer & istr;
	const Block sample;
	DataTypes data_types;
	const Context & context;
};

}
