//
// QName.h
//
// Library: XML
// Package: XML
// Module:  QName
//
// Definition of the QName class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_QName_INCLUDED
#define XML_QName_INCLUDED


#include "Poco/XML/XML.h"
#include <string>
#include <iosfwd>


namespace Poco {
namespace XML {


class XML_API QName
	/// This class represents a qualified XML name in the stream parser.
	///
	/// Note that the optional prefix is just a "syntactic sugar". In
	/// particular, it is ignored by the comparison operators and the
	/// std::ostream insertion operator.
{
public:
	QName();
	QName(const std::string& name);
	QName(const std::string& ns, const std::string& name);
	QName(const std::string& ns, const std::string& name, const std::string& prefix);
	QName(const QName& qname);
	QName(QName&& qname) noexcept;

	QName& operator = (const QName& qname);
	QName& operator = (QName&& qname) noexcept;
	void swap(QName& qname);

	const std::string& namespaceURI() const;
		/// Returns the namespace URI part of the name.

	const std::string& localName() const;
		/// Returns the local part of the name.

	const std::string& prefix() const;
		/// Returns the namespace prefix of the name.

	std::string& namespaceURI();
		/// Returns the namespace URI part of the name.

	std::string& localName();
		/// Returns the local part of the name.

	std::string& prefix();
		/// Returns the namespace prefix of the name.

	std::string toString() const;
		/// Returns a printable representation in the [<namespace>#]<name> form.

public:
	friend bool operator < (const QName& x, const QName& y);
	friend bool operator == (const QName& x, const QName& y);
	friend bool operator != (const QName& x, const QName& y);

private:
	std::string _ns;
	std::string _name;
	std::string _prefix;
};


//
// inlines
//
inline const std::string& QName::namespaceURI() const
{
	return _ns;
}


inline const std::string& QName::localName() const
{
	return _name;
}


inline const std::string& QName::prefix() const
{
	return _prefix;
}


inline std::string& QName::namespaceURI()
{
	return _ns;
}


inline std::string& QName::localName()
{
	return _name;
}


inline std::string& QName::prefix()
{
	return _prefix;
}


XML_API std::ostream& operator << (std::ostream&, const QName&);


inline bool operator < (const QName& x, const QName& y)
{
	return x._ns < y._ns || (x._ns == y._ns && x._name < y._name);
}


inline bool operator == (const QName& x, const QName& y)
{
	return x._ns == y._ns && x._name == y._name;
}


inline bool operator != (const QName& x, const QName& y)
{
	return !(x == y);
}


} } // namespace Poco::XML


#endif // XML_QName_INCLUDED
