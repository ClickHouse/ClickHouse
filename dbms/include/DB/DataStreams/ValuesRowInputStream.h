#pragma once

#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

class Block;
class Context;
class ReadBuffer;


/** Поток для чтения данных в формате VALUES (как в INSERT запросе).
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
	ValuesRowInputStream(ReadBuffer & istr_, const Context & context_);

	bool read(Block & block) override;

private:
	ReadBuffer & istr;
	const Context & context;
};

}
