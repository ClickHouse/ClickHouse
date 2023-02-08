//
// Content.h
//
// Library: XML
// Package: XML
// Module:  Content
//
// Definition of the Content enum.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_Content_INCLUDED
#define XML_Content_INCLUDED


namespace Poco {
namespace XML {


struct Content
	/// XML content model. C++11 enum class emulated for C++98.
	///
	///               element   characters  whitespaces  notes
	///     Empty     no          no        ignored
	///     Simple    no          yes       preserved    content accumulated
	///     Complex   yes         no        ignored
	///     Mixed     yes         yes       preserved
{
	enum value
	{
		Empty,
		Simple,
		Complex,
		Mixed
	};

	Content(value v)
		: _v(v)
	{
	}

	operator value() const
	{
		return _v;
	}

private:
	value _v;
};


} } // namespace Poco::XML


#endif // XML_Content_INCLUDED
