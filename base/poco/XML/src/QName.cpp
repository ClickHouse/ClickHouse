//
// QName.cpp
//
// Library: XML
// Package: XML
// Module:  QName
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/QName.h"
#include <ostream>


namespace Poco {
namespace XML {


QName::QName()
{
}


QName::QName(const std::string& name) :
	_name(name)
{
}


QName::QName(const std::string& ns, const std::string& name) :
	_ns(ns),
	_name(name)
{
}


QName::QName(const std::string& ns, const std::string& name, const std::string& prefix) :
	_ns(ns),
	_name(name),
	_prefix(prefix)
{
}


QName::QName(const QName& qname):
	_ns(qname._ns),
	_name(qname._name),
	_prefix(qname._prefix)
{
}


QName::QName(QName&& qname) noexcept:
	_ns(std::move(qname._ns)),
	_name(std::move(qname._name)),
	_prefix(std::move(qname._prefix))
{
}


QName& QName::operator = (const QName& qname)
{
	QName tmp(qname);
	swap(tmp);
	return *this;
}


QName& QName::operator = (QName&& qname) noexcept
{
	_ns = std::move(qname._ns);
	_name = std::move(qname._name);
	_prefix = std::move(qname._prefix);
	
	return *this;
}


void QName::swap(QName& qname)
{
	std::swap(_ns, qname._ns);
	std::swap(_name, qname._name);
	std::swap(_prefix, qname._prefix);
}


std::string QName::toString() const
{
	std::string r;
	if (!_ns.empty())
	{
		r += _ns;
		r += '#';
	}

	r += _name;
	return r;
}


std::ostream& operator << (std::ostream& os, const QName& qn)
{
	return os << qn.toString();
}


} } // namespace Poco::XML
