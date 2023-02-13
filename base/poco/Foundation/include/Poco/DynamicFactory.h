//
// DynamicFactory.h
//
// Library: Foundation
// Package: Core
// Module:  DynamicFactory
//
// Definition of the DynamicFactory class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DynamicFactory_INCLUDED
#define Foundation_DynamicFactory_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Instantiator.h"
#include "Poco/Exception.h"
#include "Poco/Mutex.h"
#include <map>
#include <memory>


namespace Poco {


template <class Base>
class DynamicFactory
	/// A factory that creates objects by class name.
{
public:
	typedef AbstractInstantiator<Base> AbstractFactory;

	DynamicFactory()
		/// Creates the DynamicFactory.
	{
	}

	~DynamicFactory()
		/// Destroys the DynamicFactory and deletes the instantiators for 
		/// all registered classes.
	{
		for (typename FactoryMap::iterator it = _map.begin(); it != _map.end(); ++it)
		{
			delete it->second;
		}
	}
	
	Base* createInstance(const std::string& className) const
		/// Creates a new instance of the class with the given name.
		/// The class must have been registered with registerClass.
		/// If the class name is unknown, a NotFoundException is thrown.
	{
		FastMutex::ScopedLock lock(_mutex);

		typename FactoryMap::const_iterator it = _map.find(className);
		if (it != _map.end())
			return it->second->createInstance();
		else
			throw NotFoundException(className);
	}
	
	template <class C> 
	void registerClass(const std::string& className)
		/// Registers the instantiator for the given class with the DynamicFactory.
		/// The DynamicFactory takes ownership of the instantiator and deletes
		/// it when it's no longer used.
		/// If the class has already been registered, an ExistsException is thrown
		/// and the instantiator is deleted.
	{
		registerClass(className, new Instantiator<C, Base>);
	}
	
	void registerClass(const std::string& className, AbstractFactory* pAbstractFactory)
		/// Registers the instantiator for the given class with the DynamicFactory.
		/// The DynamicFactory takes ownership of the instantiator and deletes
		/// it when it's no longer used.
		/// If the class has already been registered, an ExistsException is thrown
		/// and the instantiator is deleted.
	{
		poco_check_ptr (pAbstractFactory);

		FastMutex::ScopedLock lock(_mutex);

#ifndef POCO_ENABLE_CPP11
		std::auto_ptr<AbstractFactory> ptr(pAbstractFactory);
#else
		std::unique_ptr<AbstractFactory> ptr(pAbstractFactory);
#endif // POCO_ENABLE_CPP11

		typename FactoryMap::iterator it = _map.find(className);
		if (it == _map.end())
			_map[className] = ptr.release();
		else
			throw ExistsException(className);
	}
	
	void unregisterClass(const std::string& className)
		/// Unregisters the given class and deletes the instantiator
		/// for the class.
		/// Throws a NotFoundException if the class has not been registered.
	{
		FastMutex::ScopedLock lock(_mutex);

		typename FactoryMap::iterator it = _map.find(className);
		if (it != _map.end())
		{
			delete it->second;
			_map.erase(it);
		}
		else throw NotFoundException(className);
	}
	
	bool isClass(const std::string& className) const
		/// Returns true iff the given class has been registered.
	{
		FastMutex::ScopedLock lock(_mutex);

		return _map.find(className) != _map.end();
	}

private:
	DynamicFactory(const DynamicFactory&);
	DynamicFactory& operator = (const DynamicFactory&);

	typedef std::map<std::string, AbstractFactory*> FactoryMap;
	
	FactoryMap _map;
	mutable FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_DynamicFactory_INCLUDED
