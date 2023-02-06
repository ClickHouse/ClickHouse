//
// Struct.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Struct
//
// Definition of the Struct class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Struct_INCLUDED
#define CppParser_Struct_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/NameSpace.h"
#include <vector>
#include <set>


namespace Poco {
namespace CppParser {


class Function;


class CppParser_API Struct: public NameSpace
	/// This class represents a struct or class declaration.
{
public:
	enum Flags
	{
		FN_TEMPLATE = 1,
		FN_INLINE   = 2, // when the whole class is inlined in a c++ file
		FN_TEMPLATE_SPECIALIZATION = 4,
		FN_FINAL = 8
	};
	
	struct Base
	{
		Symbol::Access access;
		bool           isVirtual;
		std::string    name;
		Struct*        pClass;
	};
	
	typedef std::vector<Base>           BaseClasses;
	typedef BaseClasses::const_iterator BaseIterator;
	typedef std::vector<Struct*>        StructVec;
	typedef StructVec::const_iterator   DerivedIterator;
	typedef std::vector<Function*>      Functions;
	typedef std::set<Function*>         FunctionSet;
	typedef std::set<Struct*>           StructSet;

	Struct(const std::string& decl, bool isClass, NameSpace* pNameSpace);
		/// Creates the Struct.

	~Struct();
		/// Destroys the Struct.

	void addBase(const std::string&, Symbol::Access access, bool isVirtual);
		/// Adds a base class.
				
	BaseIterator baseBegin() const;
		/// Returns an iterator for iterating over all base classes.
		
	BaseIterator baseEnd() const;
		/// Returns an iterator for iterating over all base classes.
	
	void fixupBases();
		/// Adds pointers for all base classes.

	void addDerived(Struct* pClass);
		/// Adds a derived class.

	DerivedIterator derivedBegin() const;
		/// Returns an iterator for iterating over all derived classes.

	DerivedIterator derivedEnd() const;
		/// Returns an iterator for iterating over all derived classes.

	const std::string& declaration() const;
		/// Returns the declaration.
	
	int flags() const;
		/// Returns the struct's flags.

	void makeInline();
		/// Changes the class to a inline class, i.e. definition and implementation are hidden in a cpp file.
		
	void makeFinal();
		/// Makes the class final.

	bool isInline() const;
		/// Returns true if the complete class is inlined in a cpp file.

	bool isFinal() const;
		/// Returns true if the class is final.

	void constructors(Functions& functions) const;
		/// Returns all constructors, sorted by their parameter count.
	
	Function* destructor() const;
		/// Returns the destructor, or NULL if no
		/// destructor is defined.

	void methods(Symbol::Access access, Functions& functions) const;
		/// Returns all functions with the given access.
	
	void inheritedMethods(FunctionSet& functions) const;
		/// Returns all inherited methods.
	
	void bases(std::set<std::string>& bases) const;
		/// Returns all base classes.
		
	void derived(StructSet& derived) const;
		/// Returns all derived classes.	
		
	Function* findFunction(const std::string& signature) const;
		/// Finds a function with the given signature.
		
	bool hasVirtualDestructor() const;
		/// Returns true if the class CppParser_API or one if its base classes
		/// has a virtual destructor.

	bool isClass() const;
		/// Returns true iff the struct was declared as class.
		
	bool isDerived() const;
		/// Returns true iff the struct or class is derived from another struct or class.

	Symbol::Kind kind() const;
	std::string toString() const;
	
private:
	std::string _decl;
	BaseClasses _bases;
	StructVec   _derived;
	int         _flags;
	bool        _isClass;
};


//
// inlines
//
inline const std::string& Struct::declaration() const
{
	return _decl;
}


inline int Struct::flags() const
{
	return _flags;
}


inline bool Struct::isClass() const
{
	return _isClass;
}


inline void Struct::makeInline()
{
	_flags |= FN_INLINE;
}


inline void Struct::makeFinal()
{
	_flags |= FN_FINAL;
}


inline bool Struct::isInline() const
{
	return (_flags & FN_INLINE) != 0;
}


inline bool Struct::isFinal() const
{
	return (_flags & FN_FINAL) != 0;
}


inline bool Struct::isDerived() const
{
	return !_bases.empty();
}


} } // namespace Poco::CppParser


#endif // CppParser_Struct_INCLUDED
