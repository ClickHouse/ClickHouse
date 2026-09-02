//
// ProcessingInstruction.cpp
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


#include "Poco/DOM/ProcessingInstruction.h"


namespace Poco {
namespace XML {


ProcessingInstruction::ProcessingInstruction(Document* pOwnerDocument, const XMLString& target, const XMLString& data): 
	AbstractNode(pOwnerDocument),
	_target(target),
	_data(data)
{
}


ProcessingInstruction::ProcessingInstruction(Document* pOwnerDocument, const ProcessingInstruction& processingInstruction): 
	AbstractNode(pOwnerDocument, processingInstruction),
	_target(processingInstruction._target),
	_data(processingInstruction._data)
{
}


ProcessingInstruction::~ProcessingInstruction()
{
}


void ProcessingInstruction::setData(const XMLString& data)
{
	_data = data;
}


const XMLString& ProcessingInstruction::nodeName() const
{
	return _target;
}


const XMLString& ProcessingInstruction::getNodeValue() const
{
	return _data;
}


void ProcessingInstruction::setNodeValue(const XMLString& data)
{
	setData(data);
}


unsigned short ProcessingInstruction::nodeType() const
{
	return Node::PROCESSING_INSTRUCTION_NODE;
}


Node* ProcessingInstruction::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new ProcessingInstruction(pOwnerDocument, *this);
}


} } // namespace Poco::XML
