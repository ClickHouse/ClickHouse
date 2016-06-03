//
// DOMException.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/DOMException.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM DOMException class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DOMException_INCLUDED
#define DOM_DOMException_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLException.h"


namespace Poco {
namespace XML {


class XML_API DOMException: public XMLException
	/// DOM operations only raise exceptions in "exceptional" circumstances, i.e.,
	/// when an operation is impossible to perform (either for logical reasons,
	/// because data is lost, or because the implementation has become unstable).
	/// In general, DOM methods return specific error values in ordinary processing
	/// situations, such as out-of-bound errors when using NodeList.
	/// 
	/// Implementations should raise other exceptions under other circumstances.
	/// For example, implementations should raise an implementation-dependent exception
	/// if a null argument is passed when null was not expected.
{
public:
	enum
	{
		INDEX_SIZE_ERR = 1,          /// index or size is negative or greater than allowed value
		DOMSTRING_SIZE_ERR,          /// the specified range of text does not fit into a DOMString (not used)
		HIERARCHY_REQUEST_ERR,       /// a node is inserted somewhere it doesn't belong
		WRONG_DOCUMENT_ERR,          /// a node is used in a different document than the one that created it
		INVALID_CHARACTER_ERR,       /// an invalid character is specified (not used)
		NO_DATA_ALLOWED_ERR,         /// data is specified for a node which does not support data
		NO_MODIFICATION_ALLOWED_ERR, /// an attempt is made to modify an object where modifications are not allowed
		NOT_FOUND_ERR,               /// an attempt was made to reference a node in a context where it does not exist
		NOT_SUPPORTED_ERR,           /// the implementation does not support the type of object requested
		INUSE_ATTRIBUTE_ERR,         /// an attempt is made to add an attribute that is already in use elsewhere
		INVALID_STATE_ERR,           /// a parameter or an operation is not supported by the underlying object
		SYNTAX_ERR,                  /// an invalid or illegal string is specified
		INVALID_MODIFICATION_ERR,    /// an attempt is made to modify the type of the underlying object
		NAMESPACE_ERR,               /// an attempt is made to create or change an object in a way which is incorrect with regard to namespaces
		INVALID_ACCESS_ERR,          /// an attempt is made to use an object that is not, or is no longer, usable
		
		_NUMBER_OF_MESSAGES
	};

	DOMException(unsigned short code);
		/// Creates a DOMException with the given error code.
		
	DOMException(const DOMException& exc);
		/// Creates a DOMException by copying another one.
		
	~DOMException() throw();
		/// Destroys the DOMException.
		
	DOMException& operator = (const DOMException& exc);

	const char* name() const throw();
		/// Returns a static string describing the exception.

	const char* className() const throw();
		/// Returns the name of the exception class.

	Poco::Exception* clone() const;
		/// Creates an exact copy of the exception.
		
	void rethrow() const;
		/// (Re)Throws the exception.

	unsigned short code() const;
		/// Returns the DOM exception code.

protected:
	static const std::string& message(unsigned short code);
	
private:
	DOMException();
	
	unsigned short _code;
	
	static const std::string MESSAGES[_NUMBER_OF_MESSAGES];
};


//
// inlines
//
inline unsigned short DOMException::code() const
{
	return _code;
}


} } // namespace Poco::XML


#endif // DOM_DOMException_INCLUDED
