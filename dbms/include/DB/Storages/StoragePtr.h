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
		return *this;
	}
	
	IStorage* get() const
	{
		if (!ptr)
			return NULL;
		else
			return ptr->storage.get();
	}
	
	bool operator == (const IStorage * p) const
	{
		return get() == p;
	}
	
	IStorage* operator -> () const
	{
		return get();
	}

	IStorage& operator * () const
	{
		return *get();
	}

	operator IStorage* () const
	{
		return get();
	}
	
	operator bool () const
	{
		return ptr;
	}
	
	bool operator ! () const
	{
		return !ptr;
	}
};

}
