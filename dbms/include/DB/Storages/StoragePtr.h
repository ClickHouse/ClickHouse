#pragma once

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/scoped_ptr.hpp>

#include <DB/Core/Exception.h>


namespace DB
{

class IStorage;
class StorageWeakPtr;

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
	friend class StorageWeakPtr;

public:
	StoragePtr() {}
	StoragePtr(const StoragePtr & p) : ptr(p.ptr) {}
	StoragePtr(const StorageWeakPtr & p);
	
	StoragePtr& operator= (const StoragePtr & p)
	{
		ptr = p.ptr;
		return *this;
	}
	
	IStorage* get() const
	{
		if (ptr == NULL)
			return NULL;
		else
			return ptr->storage.get();
	}
	
	bool operator== (const IStorage * p) const
	{
		return get() == p;
	}
	
	IStorage* operator-> () const
	{
		return get();
	}

	IStorage& operator* () const
	{
		return *get();
	}

	operator IStorage*() const
	{
		return get();
	}
	
	operator bool() const
	{
		return bool(ptr);
	}
	
	bool operator! () const
	{
		return !bool(ptr);
	}
};

class StorageWeakPtr
{
public:
	StorageWeakPtr(const StoragePtr & p) : ptr(p.ptr) {}

	StoragePtr lock()
	{
		return StoragePtr(ptr);
	}

private:
	friend class StoragePtr;

	boost::weak_ptr<StoragePtr::Wrapper> ptr;
};

StoragePtr::StoragePtr(const StorageWeakPtr & p) : ptr(p.ptr) {}

}
