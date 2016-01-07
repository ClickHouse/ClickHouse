//
// MetaObject.h
//
// $Id: //poco/1.4/Foundation/include/Poco/MetaObject.h#1 $
//
// Library: Foundation
// Package: SharedLibrary
// Module:  ClassLoader
//
// Definition of the MetaObject class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_MetaObject_INCLUDED
#define Foundation_MetaObject_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/SingletonHolder.h"
#include <set>


namespace Poco {


template <class B>
class AbstractMetaObject
	/// A MetaObject stores some information
	/// about a C++ class. The MetaObject class
	/// is used by the Manifest class.
	/// AbstractMetaObject is a common base class
	/// for all MetaObject in a rooted class hierarchy.
	/// A MetaObject can also be used as an object
	/// factory for its class.
{
public:
	AbstractMetaObject(const char* name): _name(name)
	{
	}

	virtual ~AbstractMetaObject()
	{
		for (typename ObjectSet::iterator it = _deleteSet.begin(); it != _deleteSet.end(); ++it)
		{
			delete *it;
		}
	}

	const char* name() const
	{
		return _name;
	}

	virtual B* create() const = 0;
		/// Create a new instance of a class.
		/// Cannot be used for singletons.
		
	virtual B& instance() const = 0;
		/// Returns a reference to the only instance
		/// of the class. Used for singletons only.

	virtual bool canCreate() const = 0;
		/// Returns true iff the create method can be used
		/// to create instances of the class.
		/// Returns false if the class is a singleton.

	virtual void destroy(B* pObject) const
		/// If pObject was owned by meta object, the
		/// ownership of the deleted object is removed
		/// and the object is deleted.
	{
		typename ObjectSet::iterator it = _deleteSet.find(pObject);
		
		if (it != _deleteSet.end())
		{
			_deleteSet.erase(pObject);
			delete pObject;
		}
	}

	B* autoDelete(B* pObject) const
		/// Give ownership of pObject to the meta object.
		/// The meta object will delete all objects it owns
		/// when it is destroyed.
		///
		/// Returns pObject.
	{
		if (this->canCreate()) // guard against singleton
		{
			poco_check_ptr (pObject);
			_deleteSet.insert(pObject);
		}
		else throw InvalidAccessException("Cannot take ownership of", this->name());

		return pObject;
	}

	virtual bool isAutoDelete(B* pObject) const
		/// Returns true if the object is owned
		/// by meta object.
		///
		/// Overloaded in MetaSingleton - returns true
		/// if the class is a singleton.
	{
		return _deleteSet.find(pObject) != _deleteSet.end();
	}

private:
	AbstractMetaObject();
	AbstractMetaObject(const AbstractMetaObject&);
	AbstractMetaObject& operator = (const AbstractMetaObject&);

	typedef std::set<B*> ObjectSet;
	
	const char* _name;
	mutable ObjectSet _deleteSet;
};


template <class C, class B>
class MetaObject: public AbstractMetaObject<B>
	/// A MetaObject stores some information
	/// about a C++ class. The MetaObject class
	/// is used by the Manifest class.
	/// A MetaObject can also be used as an object
	/// factory for its class.
{
public:
	MetaObject(const char* name): AbstractMetaObject<B>(name)
	{
	}

	~MetaObject()
	{
	}

	B* create() const
	{
		return new C;
	}
	
	B& instance() const
	{
		throw InvalidAccessException("Not a singleton. Use create() to create instances of", this->name());
	}
	
	bool canCreate() const
	{
		return true;
	}
};


template <class C, class B> 
class MetaSingleton: public AbstractMetaObject<B> 
	/// A SingletonMetaObject disables the create() method
	/// and instead offers an instance() method to access
	/// the single instance of its class. 
{ 
public: 
	MetaSingleton(const char* name): AbstractMetaObject<B>(name) 
	{
	}
	
	~MetaSingleton() 
	{
	}
	
	B* create() const
	{
		throw InvalidAccessException("Cannot create instances of a singleton class. Use instance() to obtain a", this->name());
	}
	
	bool canCreate() const
	{
		return false;
	}

	B& instance() const
	{
		return *_object.get();
	}

	bool isAutoDelete(B* /*pObject*/) const
	{
		return true;
	}

private: 
	mutable SingletonHolder<C> _object; 
}; 


} // namespace Poco


#endif // Foundation_MetaObject_INCLUDED
