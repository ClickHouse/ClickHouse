//
// Array.h
//
// Library: Redis
// Package: Redis
// Module:  Array
//
// Implementation of the Array class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Redis/Array.h"


namespace Poco {
namespace Redis {


Array::Array()
{
}


Array::Array(const Array& copy): 
	_elements(copy._elements)
{
}


Array::~Array()
{
}


Array& Array::addRedisType(RedisType::Ptr value)
{
	checkNull();

	_elements.value().push_back(value);

	return *this;
}


int Array::getType(size_t pos) const
{
	if (_elements.isNull()) throw NullValueException();

	if (pos >= _elements.value().size()) throw InvalidArgumentException();

	RedisType::Ptr element = _elements.value().at(pos);
	return element->type();
}


std::string Array::toString() const
{
	return RedisTypeTraits<Array>::toString(*this);
}


} } // namespace Poco::Redis
