//
// NamedTuplesTest.cpp
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NamedTuplesTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NamedTuple.h"
#include "Poco/Exception.h"


using Poco::NamedTuple;
using Poco::Int8;
using Poco::UInt8;
using Poco::Int16;
using Poco::UInt16;
using Poco::Int32;
using Poco::UInt32;
using Poco::Int8;
using Poco::UInt8;
using Poco::Int16;
using Poco::UInt16;
using Poco::Int32;
using Poco::UInt32;
using Poco::NotFoundException;
using Poco::InvalidArgumentException;


NamedTuplesTest::NamedTuplesTest(const std::string& name): CppUnit::TestCase(name)
{
}


NamedTuplesTest::~NamedTuplesTest()
{
}


void NamedTuplesTest::testNamedTuple1()
{
	typedef NamedTuple<std::string> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 1);
	
	TupleType aTuple2("string1", "1");
	assert (aTuple2["string1"] == "1");

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.set<0>("2");
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3.length == 1);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple2()
{
	typedef NamedTuple<std::string, int> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0); 

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 2);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3.length == 2);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple3()
{
	typedef NamedTuple<std::string, 
		int, 
		bool> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false); 

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 3);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3.length == 3);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple4()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false); 

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 4);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3.length == 4);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple5()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 5);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c');
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3.length == 5);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple6()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 6);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3.length == 6);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple7()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 7);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3.length == 7);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple8()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 8);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3.length == 8);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}



void NamedTuplesTest::testNamedTuple9()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 9);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2");
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3.length == 9);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple10()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 10);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2 );
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3.length == 10);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple11()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 11);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3");
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3.length == 11);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple12()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 12);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3.length == 12);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple13()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 13);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3.length == 13);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple14()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 14);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3.length == 14);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple15()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float, 
		char> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 15);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5,
		"char2", 'c');
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["char2"] == 'c');

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3["bool2"] == false);
	assert (aTuple3.length == 15);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple16()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float, 
		char, 
		long> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);
	assert (aTuple["O"] == 0); 

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 16);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5,
		"char2", 'c',
		"long2", 999);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["char2"] == 'c');
	assert (aTuple2["long2"] == 999); 
	assert (aTuple2.length == 16);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3["bool2"] == false);
	assert (aTuple3["char2"] == 0); 
	assert (aTuple3.length == 16);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple17()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);
	assert (aTuple["O"] == 0); 
	assert (aTuple["P"] == 0);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 17);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5,
		"char2", 'c',
		"long2", 999,
		"double2", 2.5);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["char2"] == 'c');
	assert (aTuple2["long2"] == 999); 
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2.length == 17);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3["bool2"] == false);
	assert (aTuple3["char2"] == 0);
	assert (aTuple3["long2"] == 0); 
	assert (aTuple3.length == 17);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple18()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);
	assert (aTuple["O"] == 0); 
	assert (aTuple["P"] == 0);
	assert (aTuple["R"] == 0);

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 18);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5,
		"char2", 'c',
		"long2", 999,
		"double2", 2.5,
		"short2", 32700);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["char2"] == 'c');
	assert (aTuple2["long2"] == 999); 
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["short2"] == 32700);
	assert (aTuple2.length == 18);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3["bool2"] == false);
	assert (aTuple3["char2"] == 0);
	assert (aTuple3["long2"] == 0);
	assert (aTuple3["short2"] == 0); 
	assert (aTuple3.length == 18);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple19()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);
	assert (aTuple["O"] == 0); 
	assert (aTuple["P"] == 0);
	assert (aTuple["R"] == 0);
	assert (aTuple["S"] == "");

	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 19);
	
	TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5,
		"char2", 'c',
		"long2", 999,
		"double2", 2.5,
		"short2", 32700, 
		"string4", "4");
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["char2"] == 'c');
	assert (aTuple2["long2"] == 999); 
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["short2"] == 32700);
	assert (aTuple2["string4"] == "4");
	assert (aTuple2.length == 19);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3["bool2"] == false);
	assert (aTuple3["char2"] == 0);
	assert (aTuple3["long2"] == 0);
	assert (aTuple3["short2"] == 0); 
	assert (aTuple3["string4"] == "");
	assert (aTuple3.length == 19);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::testNamedTuple20()
{
	typedef NamedTuple<std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int,
		std::string, 
		int, 
		bool, 
		float, 
		char, 
		long, 
		double, 
		short, 
		std::string, 
		int> TupleType;

	TupleType aTuple; 

	assert (aTuple["A"] == "");
	assert (aTuple["B"] == 0);
	assert (aTuple["C"] == false);
	assert (aTuple["E"] == 0); 
	assert (aTuple["F"] == 0);
	assert (aTuple["H"] == 0);
	assert (aTuple["I"] == "");
	assert (aTuple["J"] == 0); 
	assert (aTuple["K"] == "");
	assert (aTuple["L"] == 0);
	assert (aTuple["M"] == false);
	assert (aTuple["O"] == 0); 
	assert (aTuple["P"] == 0);
	assert (aTuple["R"] == 0);
	assert (aTuple["S"] == "");
	assert (aTuple["T"] == 0);
	try { int POCO_UNUSED xyz; xyz = aTuple["XYZ"]; fail ("must fail"); }
	catch (NotFoundException&) { }
	assert (aTuple.length == 20);
	
   TupleType aTuple2("string1", "1", 
		"int1", 1,
		"bool1", true,
		"float1", 1.5f,
		"char1", 'c',
		"long1", 999,
		"double1", 1.5,
		"short1", 32700, 
		"string2", "2",
		"int2", 2,
		"string3", "3", 
		"int3", 3, 
		"bool2", true,
		"float2", 2.5,
		"char2", 'c',
		"long2", 999,
		"double2", 2.5,
		"short2", 32700, 
		"string4", "4",
		"int4", 4);
	assert (aTuple2["string1"] == "1");
	assert (aTuple2["int1"] == 1); 
	assert (aTuple2["bool1"] == true);
	assert (aTuple2["float1"] == 1.5);
	assert (aTuple2["char1"] == 'c');
	assert (aTuple2["long1"] == 999); 
	assert (aTuple2["double1"] == 1.5);
	assert (aTuple2["short1"] == 32700);
	assert (aTuple2["string2"] == "2");
	assert (aTuple2["int2"] == 2); 
	assert (aTuple2["string3"] == "3");
	assert (aTuple2["int3"] == 3); 
	assert (aTuple2["bool2"] == true);
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["char2"] == 'c');
	assert (aTuple2["long2"] == 999); 
	assert (aTuple2["float2"] == 2.5);
	assert (aTuple2["short2"] == 32700);
	assert (aTuple2["string4"] == "4");
	assert (aTuple2["int4"] == 4); 
	assert (aTuple2.length == 20);

	assert (aTuple != aTuple2);
	aTuple = aTuple2;
	assert (aTuple == aTuple2);
	aTuple2.get<1>()++;
	assert (aTuple < aTuple2);

	TupleType aTuple3(aTuple2.names());
	assert (aTuple3.names() == aTuple2.names());
	assert (aTuple3["string1"] == "");
	assert (aTuple3["int1"] == 0); 
	assert (aTuple3["bool1"] == false);
	assert (aTuple3["char1"] == 0);
	assert (aTuple3["long1"] == 0);
	assert (aTuple3["short1"] == 0); 
	assert (aTuple3["string2"] == "");
	assert (aTuple3["int2"] == 0);
	assert (aTuple3["string3"] == "");
	assert (aTuple3["int3"] == 0); 
	assert (aTuple3["bool2"] == false);
	assert (aTuple3["char2"] == 0);
	assert (aTuple3["long2"] == 0);
	assert (aTuple3["short2"] == 0); 
	assert (aTuple3["string4"] == "");
	assert (aTuple3["int4"] == 0);
	assert (aTuple3.length == 20);

	assert (aTuple.getName(0) == "string1");
	aTuple.setName(0, "New Name");
	assert (aTuple.getName(0) == "New Name");

	try { aTuple.setName(20, ""); fail("must fail"); }
	catch (InvalidArgumentException&) { }
}


void NamedTuplesTest::setUp()
{
}


void NamedTuplesTest::tearDown()
{
}


CppUnit::Test* NamedTuplesTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NamedTuplesTest");

	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple1);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple2);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple3);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple4);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple5);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple6);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple7);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple8);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple9);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple10);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple11);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple12);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple13);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple14);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple15);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple16);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple17);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple18);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple19);
	CppUnit_addTest(pSuite, NamedTuplesTest, testNamedTuple20);

	return pSuite;
}
