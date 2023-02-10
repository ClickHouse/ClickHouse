//
// ObjectPool.h
//
// Library: Foundation
// Package: Core
// Module:  ObjectPool
//
// Definition of the ObjectPool template class and friends.
//
// Copyright (c) 2010-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ObjectPool_INCLUDED
#define Foundation_ObjectPool_INCLUDED


#include "Poco/Poco.h"
#include "Poco/Mutex.h"
#include "Poco/Condition.h"
#include "Poco/AutoPtr.h"
#include "Poco/SharedPtr.h"
#include <vector>
#include <cctype>


namespace Poco {


template <class C, class P = C*>
class PoolableObjectFactory
	/// A PoolableObjectFactory is responsible for creating and resetting
	/// objects managed by an ObjectPool.
	///
	/// Together with an ObjectPool, a PoolableObjectFactory is used as
	/// a policy class to change the behavior of the ObjectPool when
	/// creating new objects, returning used objects back to the pool
	/// and destroying objects, when the pool itself is destroyed or
	/// shrunk.
{
public:
	P createObject()
		/// Create and return a new object.
	{
		return new C;
	}
	
	bool validateObject(P pObject)
		/// Checks whether the object is still valid
		/// and can be reused.
		///
		/// Returns true if the object is valid,
		/// false otherwise.
		///
		/// To maintain the integrity of the pool, this method
		/// must not throw an exception.
	{
		return true;
	}
	
	void activateObject(P pObject)
		/// Called before an object is handed out by the pool.
		/// Also called for newly created objects, before
		/// they are given out for the first time.
	{
	}
	
	void deactivateObject(P pObject)
		/// Called after an object has been given back to the
		/// pool and the object is still valid (a prior call
		/// to validateObject() returned true).
		///
		/// To maintain the integrity of the pool, this method
		/// must not throw an exception.
	{
	}
	
	void destroyObject(P pObject)
		/// Destroy an object.
		///
		/// To maintain the integrity of the pool, this method
		/// must not throw an exception.
	{
		delete pObject;
	}
};


template <class C>
class PoolableObjectFactory <C, Poco::AutoPtr<C> >
{
public:
	Poco::AutoPtr<C> createObject()
	{
		return new C;
	}
	
	bool validateObject(Poco::AutoPtr<C> pObject)
	{
		return true;
	}
	
	void activateObject(Poco::AutoPtr<C> pObject)
	{
	}
	
	void deactivateObject(Poco::AutoPtr<C> pObject)
	{
	}
	
	void destroyObject(Poco::AutoPtr<C> pObject)
	{
	}
};


template <class C>
class PoolableObjectFactory <C, Poco::SharedPtr<C> >
{
public:
	Poco::SharedPtr<C> createObject()
	{
		return new C;
	}
	
	bool validateObject(Poco::SharedPtr<C> pObject)
	{
		return true;
	}
	
	void activateObject(Poco::SharedPtr<C> pObject)
	{
	}
	
	void deactivateObject(Poco::SharedPtr<C> pObject)
	{
	}
	
	void destroyObject(Poco::SharedPtr<C> pObject)
	{
	}
};


template <class C, class P = C*, class F = PoolableObjectFactory<C, P> >
class ObjectPool
	/// An ObjectPool manages a pool of objects of a certain class.
	///
	/// The number of objects managed by the pool can be restricted.
	///
	/// When an object is requested from the pool:
	///   - If an object is available from the pool, an object from the pool is
	///     removed from the pool, activated (using the factory) and returned.
	///   - Otherwise, if the peak capacity of the pool has not yet been reached,
	///     a new object is created and activated, using the object factory, and returned.
	///   - If the peak capacity has already been reached, null is returned after timeout.
	///
	/// When an object is returned to the pool:
	///   - If the object is valid (checked by calling validateObject()
	///     from the object factory), the object is deactivated. If the
	///     number of objects in the pool is below the capacity,
	///     the object is added to the pool. Otherwise it is destroyed.
	///   - If the object is not valid, it is destroyed immediately.
{
public:
	ObjectPool(std::size_t capacity, std::size_t peakCapacity):
		/// Creates a new ObjectPool with the given capacity
		/// and peak capacity.
		///
		/// The PoolableObjectFactory must have a public default constructor.
		_capacity(capacity),
		_peakCapacity(peakCapacity),
		_size(0)
	{
		poco_assert (capacity <= peakCapacity);
	}
	
	ObjectPool(const F& factory, std::size_t capacity, std::size_t peakCapacity):
		/// Creates a new ObjectPool with the given PoolableObjectFactory,
		/// capacity and peak capacity. The PoolableObjectFactory must have
		/// a public copy constructor.
		_factory(factory),
		_capacity(capacity),
		_peakCapacity(peakCapacity),
		_size(0)
	{
		poco_assert (capacity <= peakCapacity);
	}
	
	~ObjectPool()
		/// Destroys the ObjectPool.
	{
		try
		{
			for (typename std::vector<P>::iterator it = _pool.begin(); it != _pool.end(); ++it)
			{
				_factory.destroyObject(*it);
			}
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
		
	P borrowObject(long timeoutMilliseconds = 0)
		/// Obtains an object from the pool, or creates a new object if
		/// possible.
		///
		/// Returns null if no object is available after timeout.
		///
		/// If activating the object fails, the object is destroyed and
		/// the exception is passed on to the caller.
	{
		Poco::FastMutex::ScopedLock lock(_mutex);

		if (!_pool.empty())
		{
			P pObject = _pool.back();
			_pool.pop_back();
			return activateObject(pObject);
		}

		if (_size >= _peakCapacity)
		{
			if (timeoutMilliseconds == 0)
			{
				return 0;
			}
			while (_size >= _peakCapacity)
			{
				if ( !_availableCondition.tryWait(_mutex, timeoutMilliseconds))
				{
					// timeout
					return 0;
				}
			}
		}

		// _size < _peakCapacity
		P pObject = _factory.createObject();
		activateObject(pObject);
		_size++;
		return pObject;
	}

	void returnObject(P pObject)
		/// Returns an object to the pool.
	{
		Poco::FastMutex::ScopedLock lock(_mutex);

		if (_factory.validateObject(pObject))
		{
			_factory.deactivateObject(pObject);
			if (_pool.size() < _capacity)
			{
				try
				{
					_pool.push_back(pObject);
					return;
				}
				catch (...)
				{
				}
			}
		}
		_factory.destroyObject(pObject);
		_size--;
		_availableCondition.signal();
	}

	std::size_t capacity() const
	{
		return _capacity;
	}
	
	std::size_t peakCapacity() const
	{
		return _peakCapacity;
	}
	
	std::size_t size() const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);
		
		return _size;
	}
	
	std::size_t available() const
	{
		Poco::FastMutex::ScopedLock lock(_mutex);

		return _pool.size() + _peakCapacity - _size;
	}

protected:
	P activateObject(P pObject)
	{
		try
		{
			_factory.activateObject(pObject);
		}
		catch (...)
		{
			_factory.destroyObject(pObject);
			throw;
		}
		return pObject;
	}
	
private:
	ObjectPool();
	ObjectPool(const ObjectPool&);
	ObjectPool& operator = (const ObjectPool&);
	
	F _factory;
	std::size_t _capacity;
	std::size_t _peakCapacity;
	std::size_t _size;
	std::vector<P> _pool;
	mutable Poco::FastMutex _mutex;
	Poco::Condition _availableCondition;
};


} // namespace Poco


#endif // Foundation_ObjectPool_INCLUDED
