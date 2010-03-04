#ifndef DBMS_DATA_STREAMS_IBLOCKOUTPUTSTREAM_H
#define DBMS_DATA_STREAMS_IBLOCKOUTPUTSTREAM_H

#include <DB/Core/Block.h>


namespace DB
{

/** Интерфейс потока для записи данных в БД или в сеть, или в консоль и т. п.
  */
class IBlockOutputStream
{
public:

	/** Записать блок.
	  */
	virtual void write(const Block & block) = 0;

	virtual ~IBlockOutputStream() {}
};

}

#endif
