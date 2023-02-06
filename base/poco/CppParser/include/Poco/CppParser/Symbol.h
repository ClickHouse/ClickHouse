//
// Symbol.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Symbol
//
// Definition of the Symbol class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Symbol_INCLUDED
#define CppParser_Symbol_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Attributes.h"
#include "Poco/Foundation.h"


namespace Poco {
namespace CppParser {


class NameSpace;


class CppParser_API Symbol
	/// This is the base class for all symbols in the symbol table.
	///
	/// Every symbol has a unique ID (int) and a namespace (which
	/// may be null).
{
public:
	enum Kind
	{
		SYM_ENUM,       /// An enumeration
		SYM_ENUM_VALUE, /// An enumeration value
		SYM_FUNCTION,   /// A (member) function
		SYM_NAMESPACE,  /// A namespace
		SYM_PARAMETER,  /// A function parameter
		SYM_STRUCT,     /// A struct or class
		SYM_TYPEDEF,    /// A typedef
		SYM_TYPEALIAS,  /// A type alias (using)
		SYM_BUILTIN,    /// A built-in type
		SYM_VARIABLE    /// A (member) variable
	};

	enum Access
	{
		ACC_PUBLIC,     /// public access
		ACC_PROTECTED,  /// protected access
		ACC_PRIVATE     /// private access
	};

	Symbol();
		/// Creates the Symbol and assigns the symbol
		/// a unique ID.

	Symbol(const std::string& name, NameSpace* pNameSpace = 0);
		/// Creates the Symbol and assigns the symbol
		/// a unique ID.

	virtual ~Symbol();
		/// Destroys the Symbol.

	int id() const;
		/// Returns the symbol's unique ID.

	const std::string& name() const;
		/// Returns the symbol's (local) name.

	NameSpace* nameSpace() const;
		/// Returns the symbol's namespace which
		/// may be null.

	void setAccess(Access v);
		/// Sets the symbol's access.

	Access getAccess() const;
		/// Returns the symbol's access.

	void setDocumentation(const std::string& text);
		/// Sets the symbol's documentation.

	void addDocumentation(const std::string& text);
		/// Adds text to the symbol's documentation.

	const std::string& getDocumentation() const;
		/// Returns the symbol's documentation.

	void setFile(const std::string& path);
		/// Sets the file where the symbol is declared.

	const std::string& getFile() const;
		/// Returns the file where the symbol is defined.

	void setLineNumber(int line);
		/// Sets the line number of the symbol's declaration.

	int getLineNumber() const;
		/// Returns the line number of the symbol's declaration.

	void setPackage(const std::string& package);
		/// Sets the symbol's package.

	const std::string& getPackage() const;
		/// Returns the symbol's package.

	void setLibrary(const std::string& library);
		/// Sets the symbol's library.

	const std::string& getLibrary() const;
		/// Returns the symbol's library.

	const Attributes& attrs() const;
		/// Returns the symbol's attributes.

	Attributes& attrs();
		/// Returns the symbol's attributes.

	const Attributes& getAttributes() const;
		/// Returns the symbol's attributes.

	void setAttributes(const Attributes& attrs);
		/// Sets the symbol's attributes.

	std::string fullName() const;
		/// Returns the symbol's fully qualified name.

	static std::string extractName(const std::string& decl);
		/// Extracts the name from the declaration.

	virtual Kind kind() const = 0;
		/// Returns the symbol's kind.

	virtual std::string toString() const = 0;
		/// Returns a string representation of the symbol.

	bool isPublic() const;
		/// Returns true iff the symbol is public.

	bool isProtected() const;
		/// Returns true iff the symbol is public.

	bool isPrivate() const;
		/// Returns true iff the symbol is public.

protected:
	static bool isIdent(char c);
	static bool hasAttr(const std::string& decl, const std::string& attr);

private:
	Symbol(const Symbol&);
	Symbol& operator = (const Symbol&);

	int         _id;
	std::string _name;
	NameSpace*  _pNameSpace;
	Access      _access;
	std::string _documentation;
	std::string _file;
	int         _line;
	std::string _package;
	std::string _library;
	Attributes  _attrs;

	static int  _nextId;
};


//
// inlines
//
inline int Symbol::id() const
{
	return _id;
}


inline const std::string& Symbol::name() const
{
	return _name;
}


inline const std::string& Symbol::getDocumentation() const
{
	return _documentation;
}


inline Symbol::Access Symbol::getAccess() const
{
	return _access;
}


inline NameSpace* Symbol::nameSpace() const
{
	return _pNameSpace;
}


inline const std::string& Symbol::getFile() const
{
	return _file;
}


inline int Symbol::getLineNumber() const
{
	return _line;
}


inline const std::string& Symbol::getPackage() const
{
	return _package;
}


inline const std::string& Symbol::getLibrary() const
{
	return _library;
}


inline const Attributes& Symbol::attrs() const
{
	return _attrs;
}


inline Attributes& Symbol::attrs()
{
	return _attrs;
}


inline bool Symbol::isPublic() const
{
	return _access == ACC_PUBLIC;
}


inline bool Symbol::isProtected() const
{
	return _access == ACC_PROTECTED;
}


inline bool Symbol::isPrivate() const
{
	return _access == ACC_PRIVATE;
}


} } // namespace Poco::CppParser


#endif // CppParser_Symbol_INCLUDED
