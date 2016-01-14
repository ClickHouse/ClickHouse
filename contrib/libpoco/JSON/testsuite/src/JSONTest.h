//
// JSONTest.h
//
// $Id: //poco/1.4/JSON/testsuite/src/JSONTest.h#1 $
//
// Definition of the JSONTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSONTest_INCLUDED
#define JSONTest_INCLUDED


#include "Poco/JSON/JSON.h"
#include "CppUnit/TestCase.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Parser.h"
#include "Poco/JSON/Query.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/JSON/Stringifier.h"
#include "Poco/JSON/ParseHandler.h"
#include "Poco/JSON/PrintHandler.h"
#include "Poco/JSON/Template.h"
#include <sstream>


class JSONTest: public CppUnit::TestCase
{
public:
	JSONTest(const std::string& name);
	~JSONTest();

	void testNullProperty();
	void testTrueProperty();
	void testFalseProperty();
	void testNumberProperty();
	void testUnsignedNumberProperty();
#if defined(POCO_HAVE_INT64)
	void testNumber64Property();
	void testUnsignedNumber64Property();
#endif
	void testStringProperty();
	void testEmptyObject();
	void testComplexObject();
	void testDoubleProperty();
	void testDouble2Property();
	void testDouble3Property();
	void testObjectProperty();
	void testObjectArray();
	void testArrayOfObjects();
	void testEmptyArray();
	void testNestedArray();
	void testNullElement();
	void testTrueElement();
	void testFalseElement();
	void testNumberElement();
	void testStringElement();
	void testEmptyObjectElement();
	void testDoubleElement();
	void testSetArrayElement();
	void testOptValue();
	void testQuery();
	void testComment();
	void testPrintHandler();
	void testStringify();
	void testStringifyPreserveOrder();

	void testValidJanssonFiles();
	void testInvalidJanssonFiles();
	void testTemplate();
	void testItunes();
	void testUnicode(); 
	void testInvalidUnicodeJanssonFiles();
	void testSmallBuffer();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	std::string getTestFilesPath(const std::string& type);

	template <typename T>
	void testNumber(T number)
	{
		std::ostringstream os;
		os << "{ \"test\" : " << number << " }";
		std::string json = os.str();
		Poco::JSON::Parser parser;
		Poco::Dynamic::Var result;

		try
		{
			result = parser.parse(json);
		}
		catch (Poco::JSON::JSONException& jsone)
		{
			std::cout << jsone.message() << std::endl;
			assert(false);
		}

		assert(result.type() == typeid(Poco::JSON::Object::Ptr));

		Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
		Poco::Dynamic::Var test = object->get("test");
		assert(test.isNumeric());
		T value = test;
		assert(value == number);

		Poco::DynamicStruct ds = *object;
		assert(!ds["test"].isEmpty());
		assert(ds["test"].isNumeric());
		assert(ds["test"] == number);

		const Poco::DynamicStruct& rds = *object;
		assert(!rds["test"].isEmpty());
		assert(rds["test"].isNumeric());
		assert(rds["test"] == number);
	}
};


#endif // JSONTest_INCLUDED
