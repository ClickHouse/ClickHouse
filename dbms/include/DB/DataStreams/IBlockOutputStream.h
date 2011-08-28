#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>


namespace DB
{

using Poco::SharedPtr;


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

typedef SharedPtr<IBlockOutputStream> BlockOutputStreamPtr;

}
