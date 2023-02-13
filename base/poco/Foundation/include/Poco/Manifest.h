//
// Manifest.h
//
// Library: Foundation
// Package: SharedLibrary
// Module:  ClassLoader
//
// Definition of the Manifest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Manifest_INCLUDED
#define Foundation_Manifest_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/MetaObject.h"
#include <map>
#include <typeinfo>


namespace Poco {


class Foundation_API ManifestBase
	/// ManifestBase is a common base class for
	/// all instantiations of Manifest.
{
public:
	ManifestBase();
	virtual ~ManifestBase();

	virtual const char* className() const = 0;
		/// Returns the type name of the manifest's class.
};


template <class B>
class Manifest: public ManifestBase
	/// A Manifest maintains a list of all classes
	/// contained in a dynamically loadable class
	/// library.
	/// Internally, the information is held
	/// in a map. An iterator is provided to
	/// iterate over all the classes in a Manifest.
{
public:
	typedef AbstractMetaObject<B> Meta;
	typedef std::map<std::string, const Meta*> MetaMap;

	class Iterator
		/// The Manifest's very own iterator class.
	{
	public:
		Iterator(const typename MetaMap::const_iterator& it)
		{
			_it = it;
		}
		Iterator(const Iterator& it)
		{
			_it = it._it;
		}
		~Iterator()
		{
		}
		Iterator& operator = (const Iterator& it)
		{
			_it = it._it;
			return *this;
		}
		inline bool operator == (const Iterator& it) const
		{
			return _it == it._it;
		}
		inline bool operator != (const Iterator& it) const
		{
			return _it != it._it;
		}
		Iterator& operator ++ () // prefix
		{
			++_it;
			return *this;
		}
		Iterator operator ++ (int) // postfix
		{
			Iterator result(_it);
			++_it;
			return result;
		}
		inline const Meta* operator * () const
		{
			return _it->second;
		}
		inline const Meta* operator -> () const
		{
			return _it->second;
		}
		
	private:
		typename MetaMap::const_iterator _it;
	};

	Manifest()
		/// Creates an empty Manifest.
	{
	}

	virtual ~Manifest()
		/// Destroys the Manifest.
	{
		clear();
	}

	Iterator find(const std::string& className) const
		/// Returns an iterator pointing to the MetaObject
		/// for the given class. If the MetaObject cannot
		/// be found, the iterator points to end().
	{
		return Iterator(_metaMap.find(className));
	}

	Iterator begin() const
	{
		return Iterator(_metaMap.begin());
	}

	Iterator end() const
	{
		return Iterator(_metaMap.end());
	}

	bool insert(const Meta* pMeta)
		/// Inserts a MetaObject. Returns true if insertion
		/// was successful, false if a class with the same
		/// name already exists.
	{
		return _metaMap.insert(typename MetaMap::value_type(pMeta->name(), pMeta)).second;
	}

	void clear()
		/// Removes all MetaObjects from the manifest.
	{
		for (typename MetaMap::iterator it = _metaMap.begin(); it != _metaMap.end(); ++it)
		{
			delete it->second;
		}
		_metaMap.clear();
	}

	int size() const
		/// Returns the number of MetaObjects in the Manifest.
	{
		return int(_metaMap.size());
	}

	bool empty() const
		/// Returns true iff the Manifest does not contain any MetaObjects.
	{
		return _metaMap.empty();
	}

	const char* className() const
	{
		return typeid(*this).name();
	}

private:
	MetaMap _metaMap;
};


} // namespace Poco


#endif // Foundation_Manifest_INCLUDED
