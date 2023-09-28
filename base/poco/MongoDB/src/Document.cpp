//
// Document.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Document
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Document.h"
#include "Poco/MongoDB/Binary.h"
#include "Poco/MongoDB/ObjectId.h"
#include "Poco/MongoDB/Array.h"
#include "Poco/MongoDB/RegularExpression.h"
#include "Poco/MongoDB/JavaScriptCode.h"
#include <sstream>


namespace Poco {
namespace MongoDB {


Document::Document()
{
}


Document::~Document()
{
}


Element::Ptr Document::get(const std::string& name) const
{
	Element::Ptr element;

	ElementSet::const_iterator it = std::find_if(_elements.begin(), _elements.end(), ElementFindByName(name));
	if (it != _elements.end())
	{
		return *it;
	}

	return element;
}


Int64 Document::getInteger(const std::string& name) const
{
	Element::Ptr element = get(name);
	if (element.isNull()) throw Poco::NotFoundException(name);

	if (ElementTraits<double>::TypeId == element->type())
	{
		ConcreteElement<double>* concrete = dynamic_cast<ConcreteElement<double>*>(element.get());
		if (concrete) return static_cast<Int64>(concrete->value());
	}
	else if (ElementTraits<Int32>::TypeId == element->type())
	{
		ConcreteElement<Int32>* concrete = dynamic_cast<ConcreteElement<Int32>*>(element.get());
		if (concrete) return concrete->value();
	}
	else if (ElementTraits<Int64>::TypeId == element->type())
	{
		ConcreteElement<Int64>* concrete = dynamic_cast<ConcreteElement<Int64>*>(element.get());
		if (concrete) return concrete->value();
	}
	throw Poco::BadCastException("Invalid type mismatch!");
}


void Document::read(BinaryReader& reader)
{
	int size;
	reader >> size;

	unsigned char type;
	reader >> type;

	while (type != '\0')
	{
		Element::Ptr element;
		
		std::string name = BSONReader(reader).readCString();

		switch (type)
		{
		case ElementTraits<double>::TypeId:
			element = new ConcreteElement<double>(name, 0);
			break;
		case ElementTraits<Int32>::TypeId:
			element = new ConcreteElement<Int32>(name, 0);
			break;
		case ElementTraits<std::string>::TypeId:
			element = new ConcreteElement<std::string>(name, "");
			break;
		case ElementTraits<Document::Ptr>::TypeId:
			element = new ConcreteElement<Document::Ptr>(name, new Document);
			break;
		case ElementTraits<Array::Ptr>::TypeId:
			element = new ConcreteElement<Array::Ptr>(name, new Array);
			break;
		case ElementTraits<Binary::Ptr>::TypeId:
			element = new ConcreteElement<Binary::Ptr>(name, new Binary);
			break;
		case ElementTraits<ObjectId::Ptr>::TypeId:
			element = new ConcreteElement<ObjectId::Ptr>(name, new ObjectId);
			break;
		case ElementTraits<bool>::TypeId:
			element = new ConcreteElement<bool>(name, false);
			break;
		case ElementTraits<Poco::Timestamp>::TypeId:
			element = new ConcreteElement<Poco::Timestamp>(name, Poco::Timestamp());
			break;
		case ElementTraits<BSONTimestamp>::TypeId:
			element = new ConcreteElement<BSONTimestamp>(name, BSONTimestamp());
			break;
		case ElementTraits<NullValue>::TypeId:
			element = new ConcreteElement<NullValue>(name, NullValue(0));
			break;
		case ElementTraits<RegularExpression::Ptr>::TypeId:
			element = new ConcreteElement<RegularExpression::Ptr>(name, new RegularExpression());
			break;
		case ElementTraits<JavaScriptCode::Ptr>::TypeId:
			element = new ConcreteElement<JavaScriptCode::Ptr>(name, new JavaScriptCode());
			break;
		case ElementTraits<Int64>::TypeId:
			element = new ConcreteElement<Int64>(name, 0);
			break;
		default:
			{
				std::stringstream ss;
				ss << "Element " << name << " contains an unsupported type 0x" << std::hex << (int) type;
				throw Poco::NotImplementedException(ss.str());
			}
		//TODO: x0F -> JavaScript code with scope
		//		xFF -> Min Key
		//		x7F -> Max Key
		}

		element->read(reader);
		_elements.push_back(element);

		reader >> type;
	}
}


std::string Document::toString(int indent) const
{
	std::ostringstream oss;

	oss << '{';

	if (indent > 0) oss << std::endl;


	for (ElementSet::const_iterator it = _elements.begin(); it != _elements.end(); ++it)
	{
		if (it != _elements.begin())
		{
			oss << ',';
			if (indent > 0) oss << std::endl;
		}

		for (int i = 0; i < indent; ++i) oss << ' ';

		oss << '"' << (*it)->name() << '"';
		oss << (indent > 0  ? " : " : ":");

		oss << (*it)->toString(indent > 0 ? indent + 2 : 0);
	}

	if (indent > 0)
	{
		oss << std::endl;
		if (indent >= 2) indent -= 2;

		for (int i = 0; i < indent; ++i) oss << ' ';
	}

	oss << '}';

	return oss.str();
}


void Document::write(BinaryWriter& writer)
{
	if (_elements.empty())
	{
		writer << 5;
	}
	else
	{
		std::stringstream sstream;
		Poco::BinaryWriter tempWriter(sstream);
		for (ElementSet::iterator it = _elements.begin(); it != _elements.end(); ++it)
		{
			tempWriter << static_cast<unsigned char>((*it)->type());
			BSONWriter(tempWriter).writeCString((*it)->name());
			Element::Ptr element = *it;
			element->write(tempWriter);
		}
		tempWriter.flush();
		
		Poco::Int32 len = static_cast<Poco::Int32>(5 + sstream.tellp()); /* 5 = sizeof(len) + 0-byte */
		writer << len;
		writer.writeRaw(sstream.str());
	}
	writer << '\0';
}


} } // namespace Poco::MongoDB
