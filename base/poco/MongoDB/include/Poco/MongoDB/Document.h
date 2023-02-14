//
// Document.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  Document
//
// Definition of the Document class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_Document_INCLUDED
#define MongoDB_Document_INCLUDED


#include "Poco/BinaryReader.h"
#include "Poco/BinaryWriter.h"
#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Element.h"
#include <algorithm>
#include <cstdlib>


namespace Poco {
namespace MongoDB {


class ElementFindByName
{
public:
	ElementFindByName(const std::string& name): 
		_name(name)
	{
	}

	bool operator()(const Element::Ptr& element)
	{
		return !element.isNull() && element->name() == _name;
	}

private:
	std::string _name;
};


class MongoDB_API Document
	/// Represents a MongoDB (BSON) document.
{
public:
	typedef SharedPtr<Document> Ptr;
	typedef std::vector<Document::Ptr> Vector;

	Document();
		/// Creates an empty Document.

	virtual ~Document();
		/// Destroys the Document.

	Document& addElement(Element::Ptr element);
		/// Add an element to the document.
		///
		/// The active document is returned to allow chaining of the add methods.

	template<typename T>
	Document& add(const std::string& name, T value)
		/// Creates an element with the given name and value and
		/// adds it to the document.
		///
		/// The active document is returned to allow chaining of the add methods.
	{
		return addElement(new ConcreteElement<T>(name, value));
	}

	Document& add(const std::string& name, const char* value)
		/// Creates an element with the given name and value and
		/// adds it to the document.
		///
		/// The active document is returned to allow chaining of the add methods.
	{
		return addElement(new ConcreteElement<std::string>(name, std::string(value)));
	}

	Document& addNewDocument(const std::string& name);
		/// Create a new document and add it to this document.
		/// Unlike the other add methods, this method returns
		/// a reference to the new document.

	void clear();
		/// Removes all elements from the document.

	void elementNames(std::vector<std::string>& keys) const;
		/// Puts all element names into std::vector.

	bool empty() const;
		/// Returns true if the document doesn't contain any documents.

	bool exists(const std::string& name);
		/// Returns true if the document has an element with the given name.

	template<typename T>
	T get(const std::string& name) const
		/// Returns the element with the given name and tries to convert
		/// it to the template type. When the element is not found, a
		/// NotFoundException will be thrown. When the element can't be
		/// converted a BadCastException will be thrown.
	{
		Element::Ptr element = get(name);
		if (element.isNull())
		{
			throw NotFoundException(name);
		}
		else
		{
			if (ElementTraits<T>::TypeId == element->type())
			{
				ConcreteElement<T>* concrete = dynamic_cast<ConcreteElement<T>* >(element.get());
				if (concrete != 0)
				{
					return concrete->value();
				}
			}
			throw BadCastException("Invalid type mismatch!");
		}
	}

	template<typename T>
	T get(const std::string& name, const T& def) const
		/// Returns the element with the given name and tries to convert
		/// it to the template type. When the element is not found, or
		/// has the wrong type, the def argument will be returned.
	{
		Element::Ptr element = get(name);
		if (element.isNull())
		{
			return def;
		}

		if (ElementTraits<T>::TypeId == element->type())
		{
			ConcreteElement<T>* concrete = dynamic_cast<ConcreteElement<T>* >(element.get());
			if (concrete != 0)
			{
				return concrete->value();
			}
		}

		return def;
	}

	Element::Ptr get(const std::string& name) const;
		/// Returns the element with the given name.
		/// An empty element will be returned when the element is not found.

	Int64 getInteger(const std::string& name) const;
		/// Returns an integer. Useful when MongoDB returns Int32, Int64
		/// or double for a number (count for example). This method will always
		/// return an Int64. When the element is not found, a
		/// Poco::NotFoundException will be thrown.

	template<typename T>
	bool isType(const std::string& name) const
		/// Returns true when the type of the element equals the TypeId of ElementTrait.
	{
		Element::Ptr element = get(name);
		if (element.isNull())
		{
			return false;
		}

		return ElementTraits<T>::TypeId == element->type();
	}

	void read(BinaryReader& reader);
		/// Reads a document from the reader

	std::size_t size() const;
		/// Returns the number of elements in the document.

	virtual std::string toString(int indent = 0) const;
		/// Returns a String representation of the document.

	void write(BinaryWriter& writer);
		/// Writes a document to the reader

protected:
	ElementSet _elements;
};


//
// inlines
//
inline Document& Document::addElement(Element::Ptr element)
{
	_elements.push_back(element);
	return *this;
}


inline Document& Document::addNewDocument(const std::string& name)
{
	Document::Ptr newDoc = new Document();
	add(name, newDoc);
	return *newDoc;
}


inline void Document::clear()
{
	_elements.clear();
}


inline bool Document::empty() const
{
	return _elements.empty();
}


inline void Document::elementNames(std::vector<std::string>& keys) const
{
	for (ElementSet::const_iterator it = _elements.begin(); it != _elements.end(); ++it)
	{
		keys.push_back((*it)->name());
	}
}


inline bool Document::exists(const std::string& name)
{
	return std::find_if(_elements.begin(), _elements.end(), ElementFindByName(name)) != _elements.end();
}


inline std::size_t Document::size() const
{
	return _elements.size();
}


// BSON Embedded Document
// spec: document
template<>
struct ElementTraits<Document::Ptr>
{
	enum { TypeId = 0x03 };

	static std::string toString(const Document::Ptr& value, int indent = 0)
	{
		return value.isNull() ? "null" : value->toString(indent);
	}
};


template<>
inline void BSONReader::read<Document::Ptr>(Document::Ptr& to)
{
	to->read(_reader);
}


template<>
inline void BSONWriter::write<Document::Ptr>(Document::Ptr& from)
{
	from->write(_writer);
}


} } // namespace Poco::MongoDB


#endif // MongoDB_Document_INCLUDED
