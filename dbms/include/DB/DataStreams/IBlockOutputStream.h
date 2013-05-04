#pragma once

#include <boost/noncopyable.hpp>

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/Storages/StoragePtr.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для записи данных в БД или в сеть, или в консоль и т. п.
  */
class IBlockOutputStream : private boost::noncopyable
{
public:
	
	IBlockOutputStream(StoragePtr owned_storage_ = StoragePtr()) : owned_storage(owned_storage_) {}

	/** Записать блок.
	  */
	virtual void write(const Block & block) = 0;

	/** Записать что-нибудь перед началом всех данных или после конца всех данных.
	  */
	virtual void writePrefix() {}
	virtual void writeSuffix() {}

	virtual ~IBlockOutputStream() {}
	
protected:
	StoragePtr owned_storage;
};

typedef SharedPtr<IBlockOutputStream> BlockOutputStreamPtr;

}
