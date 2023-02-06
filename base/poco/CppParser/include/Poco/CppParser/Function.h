//
// Function.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Function
//
// Definition of the Function class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Function_INCLUDED
#define CppParser_Function_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Decl.h"
#include <vector>


namespace Poco {
namespace CppParser {


class Parameter;


class CppParser_API Function: public Decl
	/// This class represents a (member) function declaration.
{
public:
	enum Flags
	{
		FN_STATIC       = 1,   /// The function is static.
		FN_VIRTUAL      = 2,   /// The function is virtual.
		FN_INLINE       = 4,   /// The function is inline.
		FN_CONST        = 8,   /// The function is const.
		FN_TEMPLATE     = 16,  /// The function is a template.
		FN_PURE_VIRTUAL = 32,  /// The function is pure virtual.
		FN_FINAL        = 64,  /// The function is final.
		FN_OVERRIDE     = 128, /// The function is override.
		FN_NOEXCEPT     = 256, /// The function is noexcept.
		FN_DEFAULT      = 512, /// The function is default.
		FN_DELETE       = 1024 /// The function has been deleted.
	};
	
	typedef std::vector<Parameter*> Parameters;
	typedef Parameters::const_iterator Iterator;

	Function(const std::string& decl, NameSpace* pNameSpace);
		/// Creates the Function.

	~Function();
		/// Destroys the Function.

	void addParameter(Parameter* pParam);
		/// Adds a parameter to the function.

	const std::string& getReturnParameter() const;

	Iterator begin() const;
		/// Returns an iterator for iterating over the Function's Parameter's.

	Iterator end() const;
		/// Returns an iterator for iterating over the Function's Parameter's.
	
	void makeInline();
		/// Sets the FN_INLINE flag.
	
	void makeConst();
		/// Sets the FN_CONST flag.
		
	void makePureVirtual();
		/// Sets the FN_PURE_VIRTUAL flag.
		
	void makeFinal();
		/// Sets the FN_FINAL flag.
		
	void makeOverride();
		/// Sets the FN_OVERRIDE flag.
		
	void makeNoexcept();
		/// Sets the FN_NOEXCEPT flag.

	void makeDefault();
		/// Sets the FN_DEFAULT flag.
	
	void makeDelete();
		/// Sets the FN_DELETE flag.
	
	int flags() const;
		/// Returns the function's flags.
		
	bool isConstructor() const;
		/// Returns true iff the function is a constructor.
		
	bool isDestructor() const;
		/// Returns true iff the function is a destructor.

	bool isMethod() const;
		/// Returns true iff the function is a method (it's part of
		/// a Struct and it's neither a constructor nor a destructor).
		
	bool isFunction() const;
		/// Returns true iff the function is not a member of a class
		/// (a freestanding function).

	bool isConst() const;
		/// Returns true iff the method is const.
		
	int countParameters() const;
		/// Returns the number of parameters.
	
	std::string signature() const;
		/// Returns the signature of the function.
		
	bool isVirtual() const;
		/// Returns true if the method is virtual. Also examines base
		/// classes to check for a virtual function with the same
		/// signature.
		
	Function* getOverridden() const;
		/// If the function is virtual and overrides a function in a
		/// base class, the base class function is returned.
		/// Otherwise, null is returned.
		
	Symbol::Kind kind() const;
	std::string toString() const;

private:
	Parameters  _params;
	int         _flags;
	std::string _retParam;
};


//
// inlines
//
inline int Function::flags() const
{
	return _flags;
}


inline const std::string& Function::getReturnParameter() const
{
	return _retParam;
}


inline bool Function::isConst() const
{
	return (flags() & FN_CONST) != 0;
}


} } // namespace Poco::CppParser


#endif // CppParser_Function_INCLUDED
