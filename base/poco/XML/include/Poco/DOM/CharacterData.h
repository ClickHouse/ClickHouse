//
// CharacterData.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM CharacterData class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_CharacterData_INCLUDED
#define DOM_CharacterData_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractNode.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API CharacterData: public AbstractNode
	/// The CharacterData interface extends Node with a set of attributes and methods
	/// for accessing character data in the DOM. For clarity this set is defined
	/// here rather than on each object that uses these attributes and methods.
	/// No DOM objects correspond directly to CharacterData, though Text and others
	/// do inherit the interface from it. All offsets in this interface start from 0.
	/// 
	/// Text strings in the DOM are represented in either UTF-8 (if XML_UNICODE_WCHAR_T is
	/// not defined) or in UTF-16 (if XML_UNICODE_WCHAR_T is defined).
	/// Indexing on character data is done in XMLChar units.
{
public:
	const XMLString& data() const;
		/// Returns the character data of the node that 
		/// implements the interface.
	
	const XMLString& getData() const;
		/// Returns the character data of the node that 
		/// implements the interface.

	void setData(const XMLString& data);
		/// Sets the character data of the node that
		/// implements the interface.

	unsigned long length() const;
		/// Returns the number of XMLChars that are available
		/// through getData and substringData. This may have the
		/// value zero.

	XMLString substringData(unsigned long offset, unsigned long count) const;
		/// Extracts a range of data from the node.
		/// If offset and count exceeds the length, then all
		/// the characters to the end of the data are returned.

	void appendData(const XMLString& arg);
		/// Append the string to the end of the character data
		/// of the node.

	void insertData(unsigned long offset, const XMLString& arg);
		/// Insert a string at the specified character offset.

	void deleteData(unsigned long offset, unsigned long count);
		/// Remove a range of characters from the node.

	void replaceData(unsigned long offset, unsigned long count, const XMLString& arg);
		/// Replace the characters starting at the specified character
		/// offset with the specified string.

	// Non-standard extensions
	XMLString trimmedData() const;
		/// Returns the character data of that node with
		/// all surrounding whitespace removed.
		///
		/// This method is an extension to the W3C Document Object Model.

	// Node
	const XMLString& getNodeValue() const;
	void setNodeValue(const XMLString& value);

protected:
	CharacterData(Document* pOwnerDocument, const XMLString& data);
	CharacterData(Document* pOwnerDocument, const CharacterData& data);
	~CharacterData();

private:
	XMLString _data;
};


//
// inlines
//
inline const XMLString& CharacterData::data() const
{
	return _data;
}


inline const XMLString& CharacterData::getData() const
{
	return _data;
}


inline unsigned long CharacterData::length() const
{
	return (unsigned long) _data.length();
}


} } // namespace Poco::XML


#endif // DOM_CharacterData_INCLUDED
