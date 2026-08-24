//
// NamePool.cpp
//
// Library: XML
// Package: XML
// Module:  NamePool
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/NamePool.h"
#include "Poco/Exception.h"
#include "Poco/Random.h"


namespace Poco {
namespace XML {


class NamePoolItem
{
public:
	NamePoolItem(): _used(false)
	{
	}
	
	~NamePoolItem()
	{
	}
	
	bool set(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName)
	{
		if (!_used)
		{
			_name.assign(qname, namespaceURI, localName);
			_used = true;
			return true;
		}
		else return _name.equals(qname, namespaceURI, localName);
	}
	
	const Name& get() const
	{
		return _name;
	}
	
	bool used() const
	{
		return _used;
	}
	
private:
	Name _name;
	bool _used;
};


NamePool::NamePool(unsigned long size): 
	_size(size),
	_salt(0),
	_rc(1)
{
	poco_assert (size > 1);

	_pItems = new NamePoolItem[size];
	
	Poco::Random rnd;
	rnd.seed();
	_salt = rnd.next();
}


NamePool::~NamePool()
{
	delete [] _pItems;
}


void NamePool::duplicate()
{
	++_rc;
}


void NamePool::release()
{
	if (--_rc == 0)
		delete this;
}


const Name& NamePool::insert(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName)
{
	unsigned long i = 0;
	unsigned long n = (hash(qname, namespaceURI, localName) ^ _salt) % _size;

	while (!_pItems[n].set(qname, namespaceURI, localName) && i++ < _size) 
		n = (n + 1) % _size;
		
	if (i > _size) throw Poco::PoolOverflowException("XML name pool");

	return _pItems[n].get();
}


const Name& NamePool::insert(const Name& name)
{
	return insert(name.qname(), name.namespaceURI(), name.localName());
}


unsigned long NamePool::hash(const XMLString& qname, const XMLString& namespaceURI, const XMLString& localName)
{
	unsigned long h = 0;
	XMLString::const_iterator it  = qname.begin();
	XMLString::const_iterator end = qname.end();
	while (it != end) h = (h << 5) + h + (unsigned long) *it++;
	it =  namespaceURI.begin();
	end = namespaceURI.end();
	while (it != end) h = (h << 5) + h + (unsigned long) *it++;
	it  = localName.begin();
	end = localName.end();
	while (it != end) h = (h << 5) + h + (unsigned long) *it++;
	return h;
}


} } // namespace Poco::XML
