//
// ProcessingInstruction.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM ProcessingInstruction class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_ProcessingInstruction_INCLUDED
#define DOM_ProcessingInstruction_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractNode.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API ProcessingInstruction: public AbstractNode
	/// The ProcessingInstruction interface represents a "processing instruction",
	/// used in XML as a way to keep processor-specific information in the text
	/// of the document.
{
public:
	const XMLString& target() const;
		/// Returns the target of this processing instruction.
		/// XML defines this as being the first token following
		/// the markup that begins the processing instruction.

	const XMLString& data() const;
		/// Returns the content of this processing instruction. This is from the first non 
		/// white space character after the target to the character immediately preceding
		/// the ?>.

	const XMLString& getData() const;
		/// Returns the content of this processing instruction. This is from the first non 
		/// white space character after the target to the character immediately preceding
		/// the ?>.

	void setData(const XMLString& data);
		/// Sets the content of this processing instruction.

	// Node
	const XMLString& nodeName() const;
	const XMLString& getNodeValue() const;
	void setNodeValue(const XMLString& data);
	unsigned short nodeType() const;

protected:
	ProcessingInstruction(Document* pOwnerDocument, const XMLString& target, const XMLString& data);
	ProcessingInstruction(Document* pOwnerDocument, const ProcessingInstruction& processingInstruction);
	~ProcessingInstruction();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	XMLString _target;
	XMLString _data;
	
	friend class Document;
};


//
// inlines
//
inline const XMLString& ProcessingInstruction::target() const
{
	return _target;
}


inline const XMLString& ProcessingInstruction::data() const
{
	return _data;
}


inline const XMLString& ProcessingInstruction::getData() const
{
	return _data;
}


} } // namespace Poco::XML


#endif // DOM_ProcessingInstruction_INCLUDED
