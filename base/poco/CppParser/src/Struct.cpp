//
// Struct.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  Struct
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Struct.h"
#include "Poco/CppParser/Function.h"
#include "Poco/String.h"
#include <algorithm>
#include <sstream>
#include <cstddef>


namespace Poco {
namespace CppParser {


Struct::Struct(const std::string& decl, bool isClass, NameSpace* pNameSpace):
	NameSpace(extractName(decl), pNameSpace),
	_decl(decl),
	_flags(0),
	_isClass(isClass)
{
	std::size_t pos = decl.find("template");
	if (pos != std::string::npos)
	{
		_flags |= FN_TEMPLATE;
		std::size_t templTypeStart = decl.find("<", pos);
		std::size_t templTypeEnd = decl.find(">", templTypeStart);
		if (templTypeStart != std::string::npos && templTypeEnd != std::string::npos)
		{
			std::string templParam = decl.substr(templTypeStart+1, templTypeEnd-templTypeStart-1);
			Poco::trimInPlace(templParam);
			if (templParam.empty())
				_flags |= FN_TEMPLATE_SPECIALIZATION;
		}
	}
}


Struct::~Struct()
{
}


void Struct::addBase(const std::string& name, Symbol::Access access, bool isVirtual)
{
	Base base;
	base.name      = name;
	base.access    = access;
	base.isVirtual = isVirtual;
	base.pClass = 0;
	_bases.push_back(base);
}

	
Struct::BaseIterator Struct::baseBegin() const
{
	return _bases.begin();
}


Struct::BaseIterator Struct::baseEnd() const
{
	return _bases.end();
}


void Struct::addDerived(Struct* pClass)
{
	poco_check_ptr (pClass);
	
	_derived.push_back(pClass);
}


void Struct::fixupBases()
{
	for (BaseClasses::iterator it = _bases.begin(); it != _bases.end(); ++it)
	{
		it->pClass = dynamic_cast<Struct*>(lookup(it->name));
		if (it->pClass)
			it->pClass->addDerived(this);
	}
}


Struct::DerivedIterator Struct::derivedBegin() const
{
	return _derived.begin();
}


Struct::DerivedIterator Struct::derivedEnd() const
{
	return _derived.end();
}


namespace
{
	struct CPLT
	{
		bool operator() (const Function* pF1, const Function* pF2) const
		{
			int pc1 = pF1->countParameters();
			int pc2 = pF2->countParameters();
			return pc1 < pc2;
		}
	};
};


void Struct::constructors(Functions& functions) const
{
	for (NameSpace::Iterator it = begin(); it != end(); ++it)
	{
		Function* pFunc = dynamic_cast<Function*>(it->second);
		if (pFunc && pFunc->isConstructor())
			functions.push_back(pFunc);
	}
	std::sort(functions.begin(), functions.end(), CPLT());
}


void Struct::bases(std::set<std::string>& bases) const
{
	for (BaseIterator it = baseBegin(); it != baseEnd(); ++it)
	{
		if (it->pClass)
		{
			bases.insert(it->pClass->fullName());
			it->pClass->bases(bases);
		}
		else bases.insert(it->name);
	}
}


Function* Struct::destructor() const
{
	for (NameSpace::Iterator it = begin(); it != end(); ++it)
	{
		Function* pFunc = dynamic_cast<Function*>(it->second);
		if (pFunc && pFunc->isDestructor())
			return pFunc;
	}
	return 0;
}


void Struct::methods(Symbol::Access access, Functions& functions) const
{
	for (NameSpace::Iterator it = begin(); it != end(); ++it)
	{
		Function* pFunc = dynamic_cast<Function*>(it->second);
		if (pFunc && pFunc->getAccess() == access && pFunc->isMethod())
			functions.push_back(pFunc);
	}
}


void Struct::inheritedMethods(FunctionSet& functions) const
{
	for (BaseIterator it = baseBegin(); it != baseEnd(); ++it)
	{
		Struct* pBase = it->pClass;
		if (pBase)
		{
			for (NameSpace::Iterator itm = pBase->begin(); itm != pBase->end(); ++itm)
			{
				Function* pFunc = dynamic_cast<Function*>(itm->second);
				if (pFunc && pFunc->getAccess() != Symbol::ACC_PRIVATE && pFunc->isMethod())
					functions.insert(pFunc);
			}
			pBase->inheritedMethods(functions);
		}
	}
}


void Struct::derived(StructSet& derived) const
{
	for (DerivedIterator it = derivedBegin(); it != derivedEnd(); ++it)
	{
		derived.insert(*it);
		(*it)->derived(derived);
	}
}


Symbol::Kind Struct::kind() const
{
	return Symbol::SYM_STRUCT;
}


Function* Struct::findFunction(const std::string& signature) const
{
	for (NameSpace::Iterator it = begin(); it != end(); ++it)
	{
		Function* pFunc = dynamic_cast<Function*>(it->second);
		if (pFunc && pFunc->signature() == signature)
			return pFunc;
	}
	for (BaseIterator it = baseBegin(); it != baseEnd(); ++it)
	{
		if (it->pClass)
		{
			Function* pFunc = it->pClass->findFunction(signature);
			if (pFunc) return pFunc;
		}
	}
	return 0;
}


bool Struct::hasVirtualDestructor() const
{
	Function* pFunc = destructor();
	if (pFunc && (pFunc->flags() & Function::FN_VIRTUAL))
		return true;
	for (BaseIterator it = baseBegin(); it != baseEnd(); ++it)
	{
		if (it->pClass && it->pClass->hasVirtualDestructor())
			return true;
	}
	return false;
}


std::string Struct::toString() const
{
	std::ostringstream ostr;
	ostr << declaration() << "\n{\n";
	for (Iterator it = begin(); it != end(); ++it)
	{
		ostr << it->second->fullName() << "\n";
		ostr << it->second->toString() << "\n";
	}
	ostr << "};\n";
	return ostr.str();
}


} } // namespace Poco::CppParser
