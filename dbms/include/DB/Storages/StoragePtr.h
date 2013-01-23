#pragma once

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/scoped_ptr.hpp>

#include <DB/Core/Exception.h>


namespace DB
{

class IStorage;


class StoragePtr
{
private:
	/// Содержит IStorage. В деструкторе при необходимости вызывает IStorage::dropImpl() перед уничтожением IStorage.
	struct Wrapper
	{
		Wrapper();
		Wrapper(IStorage * s);
		
		boost::scoped_ptr<IStorage> storage;
		
		~Wrapper();
	};
	
	
	StoragePtr(boost::weak_ptr<Wrapper> p) : ptr(p) {}
	
	boost::shared_ptr<Wrapper> ptr;
	
	friend class IStorage;

public:
	StoragePtr() {}
	StoragePtr(const StoragePtr & p) : ptr(p.ptr) {}
	
	StoragePtr& operator = (const StoragePtr & p)
	{
		ptr = p.ptr;
	}
	
	bool operator == (const IStorage * p)
	{
		return ptr->storage.get() == p;
	}
	
	IStorage* operator -> () const
	{
		return ptr->storage.get();
	}

	IStorage& operator * () const
	{
		return *ptr->storage.get();
	}

	operator IStorage* () const
	{
		return ptr->storage.get();
	}
	
	bool operator ! () const
	{
		return !ptr->storage;
	}
};

}
