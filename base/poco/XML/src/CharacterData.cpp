//
// CharacterData.cpp
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/CharacterData.h"
#include "Poco/DOM/DOMException.h"
#include "Poco/String.h"


namespace Poco {
namespace XML {


CharacterData::CharacterData(Document* pOwnerDocument, const XMLString& data): 
	AbstractNode(pOwnerDocument),
	_data(data)
{
}


CharacterData::CharacterData(Document* pOwnerDocument, const CharacterData& data): 
	AbstractNode(pOwnerDocument, data),
	_data(data._data)
{
}


CharacterData::~CharacterData()
{
}


void CharacterData::setData(const XMLString& data)
{
	if (events())
	{
		XMLString oldData = _data;
		_data = data;
		dispatchCharacterDataModified(oldData, _data);
	}
	else
	{
		_data = data;
	}
}


XMLString CharacterData::substringData(unsigned long offset, unsigned long count) const
{
	if (offset >= _data.length())
		throw DOMException(DOMException::INDEX_SIZE_ERR);

	return _data.substr(offset, count);
}


void CharacterData::appendData(const XMLString& arg)
{
	if (events())
	{
		XMLString oldData = _data;
		_data.append(arg);
		dispatchCharacterDataModified(oldData, _data);
	}
	else
	{
		_data.append(arg);
	}
}


void CharacterData::insertData(unsigned long offset, const XMLString& arg)
{
	if (offset > _data.length())
		throw DOMException(DOMException::INDEX_SIZE_ERR);

	if (events())
	{
		XMLString oldData = _data;
		_data.insert(offset, arg);
		dispatchCharacterDataModified(oldData, _data);
	}
	else
	{
		_data.insert(offset, arg);
	}
}


void CharacterData::deleteData(unsigned long offset, unsigned long count)
{
	if (offset >= _data.length())
		throw DOMException(DOMException::INDEX_SIZE_ERR);

	if (events())
	{
		XMLString oldData = _data;
		_data.replace(offset, count, EMPTY_STRING);
		dispatchCharacterDataModified(oldData, _data);
	}
	else
		_data.replace(offset, count, EMPTY_STRING);
}


void CharacterData::replaceData(unsigned long offset, unsigned long count, const XMLString& arg)
{
	if (offset >= _data.length())
		throw DOMException(DOMException::INDEX_SIZE_ERR);

	if (events())
	{
		XMLString oldData = _data;
		_data.replace(offset, count, arg);
		dispatchCharacterDataModified(oldData, _data);
	}
	else
	{
		_data.replace(offset, count, arg);
	}
}


const XMLString& CharacterData::getNodeValue() const
{
	return _data;
}


void CharacterData::setNodeValue(const XMLString& value)
{
	setData(value);
}


XMLString CharacterData::trimmedData() const
{
	return Poco::trim(_data);
}


} } // namespace Poco::XML
