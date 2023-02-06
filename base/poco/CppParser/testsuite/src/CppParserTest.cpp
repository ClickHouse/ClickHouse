//
// CppParserTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppParserTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/CppParser/Utility.h"
#include "Poco/CppParser/Symbol.h"
#include <iostream>


using namespace Poco::CppParser;


std::string linker("cl");
std::string options("/I \"C:\\Program Files\\Microsoft Visual Studio 8\\VC\\INCLUDE\", "
				"/I \"C:\\Program Files\\Microsoft Visual Studio 8\\VC\\PlatformSDK\\include\", "
				"/I \"p:\\poco\\Foundation\\include\", "
				"/I \"p:\\poco\\XML\\include\", "
				"/I \"p:\\poco\\Util\\include\", "
				"/I \"p:\\poco\\Net\\include\", "
				"/D \"WIN32\", "
				"/D \"_DEBUG\", "
				"/D \"_WINDOWS\", "
				"/D \"_MBCS\", "
				"/C, /P, /TP");
std::string path("C:\\Program Files\\Microsoft Visual Studio 8\\Common7\\IDE;C:\\Program Files\\Microsoft Visual Studio 8\\VC\\BIN;C:\\Program Files\\Microsoft Visual Studio 8\\Common7\\Tools;;C:\\Program Files\\Microsoft Visual Studio 8\\Common7\\Tools\\bin");

CppParserTest::CppParserTest(const std::string& name): CppUnit::TestCase(name)
{
}


CppParserTest::~CppParserTest()
{
}


void CppParserTest::testParseDir()
{
	//FIXME: implement some proper tests
	return;
	NameSpace::SymbolTable st;
	std::vector<std::string> inc;
	inc.push_back("./*.h");
	std::vector<std::string> exc;
	Utility::parseDir(inc, exc, st, linker, options, path);
	
	NameSpace::SymbolTable::const_iterator it = st.begin();
	NameSpace::SymbolTable::const_iterator itEnd = st.end();
	for (it; it != itEnd; ++it)
	{
		std::cout << it->first << ": ";
		Symbol* pSym = it->second;
		std::cout << pSym->name() << ", " << pSym->getDocumentation() << "\n";
		
	}
}


void CppParserTest::testExtractName()
{
	std::string decl("int _var");
	std::string name = Symbol::extractName(decl);
	assert (name == "_var");
	
	decl = "void func(int arg1, int arg2)";
	name = Symbol::extractName(decl);
	assert (name == "func");
	
	decl = "const std::vector<NS::MyType>* var";
	name = Symbol::extractName(decl);
	assert (name == "var");
	
	decl = "const std::vector<NS::MyType>* func(int arg) = 0";
	name = Symbol::extractName(decl);
	assert (name == "func");

	decl = "int (*func)(int, const std::string&)";
	name = Symbol::extractName(decl);
	assert (name == "func");

	decl = "int ( * func )(int, const std::string&)";
	name = Symbol::extractName(decl);
	assert (name == "func");

	decl = "template <typename A, typename B> B func(A a, B b)";
	name = Symbol::extractName(decl);
	assert (name == "func");
	
	decl = "template <typename A, typename B> class Class";
	name = Symbol::extractName(decl);
	assert (name == "Class");

	decl = "template <> class Class<int, std::string>";
	name = Symbol::extractName(decl);
	assert (name == "Class");

	decl = "template <> class Class <int, std::string>";
	name = Symbol::extractName(decl);
	assert (name == "Class");

	decl = "template <> class Class<int, MyTemplate<int> >";
	name = Symbol::extractName(decl);
	assert (name == "Class");
}


void CppParserTest::setUp()
{
}


void CppParserTest::tearDown()
{
}


CppUnit::Test* CppParserTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("CppParserTest");

	CppUnit_addTest(pSuite, CppParserTest, testParseDir);
	CppUnit_addTest(pSuite, CppParserTest, testExtractName);

	return pSuite;
}
