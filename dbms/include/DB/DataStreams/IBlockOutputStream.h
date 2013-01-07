#pragma once

#include <boost/noncopyable.hpp>

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для записи данных в БД или в сеть, или в консоль и т. п.
  */
class IBlockOutputStream : private boost::noncopyable
{
public:

	/** Записать блок.
	  */
	virtual void write(const Block & block) = 0;

	/** Записать что-нибудь перед началом всех данных или после конца всех данных.
	  */
	virtual void writePrefix() {}
	virtual void writeSuffix() {}

	/** Создать копию объекта.
	  * Предполагается, что функция вызывается только до использования объекта (сразу после создания, до вызова других методов),
	  *  только для того, чтобы можно было преобразовать параметр, переданный по ссылке в shared ptr.
	  */
	virtual SharedPtr<IBlockOutputStream> clone() = 0;

	virtual ~IBlockOutputStream() {}
};

typedef SharedPtr<IBlockOutputStream> BlockOutputStreamPtr;

}
