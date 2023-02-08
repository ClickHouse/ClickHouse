//
// CDATASection.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM CDATASection class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_CDATASection_INCLUDED
#define DOM_CDATASection_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/Text.h"


namespace Poco {
namespace XML {


class XML_API CDATASection: public Text
	/// CDATA sections are used to escape blocks of text containing characters that
	/// would otherwise be regarded as markup. The only delimiter that is recognized
	/// in a CDATA section is the "]]>" string that ends the CDATA section. CDATA
	/// sections cannot be nested. Their primary purpose is for including material
	/// such as XML fragments, without needing to escape all the delimiters.
	/// 
	/// The DOMString attribute of the Text node holds the text that is contained
	/// by the CDATA section. Note that this may contain characters that need to
	/// be escaped outside of CDATA sections and that, depending on the character
	/// encoding ("charset") chosen for serialization, it may be impossible to write
	/// out some characters as part of a CDATA section.
	/// 
	/// The CDATASection interface inherits from the CharacterData interface through
	/// the Text interface. Adjacent CDATASection nodes are not merged by use of
	/// the normalize method on the Element interface.
	/// 
	/// Note: Because no markup is recognized within a CDATASection, character numeric
	/// references cannot be used as an escape mechanism when serializing. Therefore,
	/// action needs to be taken when serializing a CDATASection with a character
	/// encoding where some of the contained characters cannot be represented. Failure
	/// to do so would not produce well-formed XML.
	/// One potential solution in the serialization process is to end the CDATA
	/// section before the character, output the character using a character reference
	/// or entity reference, and open a new CDATA section for any further characters
	/// in the text node. Note, however, that some code conversion libraries at
	/// the time of writing do not return an error or exception when a character
	/// is missing from the encoding, making the task of ensuring that data is not
	/// corrupted on serialization more difficult.
{
public:
	// Text
	Text* splitText(unsigned long offset);

	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

protected:
	CDATASection(Document* pOwnerDocument, const XMLString& data);
	CDATASection(Document* pOwnerDocument, const CDATASection& sec);
	~CDATASection();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	static const XMLString NODE_NAME;
	
	friend class Document;
};


} } // namespace Poco::XML


#endif // DOM_CDATASection_INCLUDED
