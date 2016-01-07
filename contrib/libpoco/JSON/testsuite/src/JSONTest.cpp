//
// JSONTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/JSONTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "JSONTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Path.h"
#include "Poco/Environment.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/Glob.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/Latin1Encoding.h"
#include "Poco/TextConverter.h"
#include "Poco/Nullable.h"
#include "Poco/Dynamic/Struct.h"
#include <set>
#include <iostream>


using namespace Poco::JSON;
using namespace Poco::Dynamic;
using Poco::DynamicStruct;


JSONTest::JSONTest(const std::string& name): CppUnit::TestCase("JSON")
{

}


JSONTest::~JSONTest()
{

}


void JSONTest::setUp()
{
}


void JSONTest::tearDown()
{
}


void JSONTest::testNullProperty()
{
	std::string json = "{ \"test\" : null }";
	Parser parser;

	Var result;
	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}
	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	assert(object->isNull("test"));
	Var test = object->get("test");
	assert(test.isEmpty());

	Poco::Nullable<int> test2 = object->getNullableValue<int>("test");
	assert(test2.isNull());

	DynamicStruct ds = *object;
	assert (ds["test"].isEmpty());

	const DynamicStruct& rds = *object;
	assert (rds["test"].isEmpty());
}


void JSONTest::testTrueProperty()
{
	std::string json = "{ \"test\" : true }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.type() == typeid(bool));
	bool value = test;
	assert(value);

	DynamicStruct ds = *object;
	assert (!ds["test"].isEmpty());
	assert (ds["test"]);

	const DynamicStruct& rds = *object;
	assert (!rds["test"].isEmpty());
	assert (rds["test"]);
}


void JSONTest::testFalseProperty()
{
	std::string json = "{ \"test\" : false }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.type() == typeid(bool));
	bool value = test;
	assert(!value);

	DynamicStruct ds = *object;
	assert (!ds["test"].isEmpty());
	assert (!ds["test"]);

	const DynamicStruct& rds = *object;
	assert (!rds["test"].isEmpty());
	assert (!rds["test"]);
}


void JSONTest::testNumberProperty()
{
	testNumber(1969);
	testNumber(-1969);
	testNumber(1969.5);
	testNumber(-1969.5);
}


void JSONTest::testUnsignedNumberProperty()
{
	// 4294967295 == unsigned(-1)
	std::string json = "{ \"test\" : 4294967295 }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.isNumeric());
	assert(test.isInteger());
	Poco::UInt32 value = test;
	assert(value == -1);

	DynamicStruct ds = *object;
	assert (!ds["test"].isEmpty());
	assert (ds["test"].isNumeric());
	assert (ds["test"].isInteger());
	assert (ds["test"] == 4294967295u);
	value = ds["test"];
	assert(value == -1);

	const DynamicStruct& rds = *object;
	assert (!rds["test"].isEmpty());
	assert (rds["test"].isNumeric());
	assert (rds["test"].isInteger());
	assert (rds["test"] == 4294967295u);
	value = rds["test"];
	assert(value == -1);
}

#if defined(POCO_HAVE_INT64)


void JSONTest::testNumber64Property()
{
	std::string json = "{ \"test\" : -5000000000000000 }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object object = *result.extract<Object::Ptr>();
	Var test = object.get("test");
	assert(test.isInteger());
	Poco::Int64 value = test;
	assert(value == -5000000000000000);

	DynamicStruct ds = object;
	assert (!ds["test"].isEmpty());
	assert (ds["test"].isNumeric());
	assert (ds["test"].isInteger());
	assert (ds["test"] == -5000000000000000);
	value = ds["test"];
	assert(value == -5000000000000000);

	const DynamicStruct& rds = object;
	assert (!rds["test"].isEmpty());
	assert (rds["test"].isNumeric());
	assert (rds["test"].isInteger());
	assert (rds["test"] == -5000000000000000);
	value = rds["test"];
	assert(value == -5000000000000000);
}


void JSONTest::testUnsignedNumber64Property()
{
	// 18446744073709551615 == UInt64(-1)
	std::string json = "{ \"test\" : 18446744073709551615 }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.isInteger());
	Poco::UInt64 value = test;
	assert(value == -1);
/* TODO: clang has trouble here
	DynamicStruct ds = *object;
	assert (!ds["test"].isEmpty());
	assert (ds["test"].isNumeric());
	assert (ds["test"].isInteger());
	assert (ds["test"] == 18446744073709551615);
	value = ds["test"];
	assert(value == -1);

	const DynamicStruct& rds = *object;
	assert (!rds["test"].isEmpty());
	assert (rds["test"].isNumeric());
	assert (rds["test"].isInteger());
	assert (rds["test"] == 18446744073709551615);
	value = rds["test"];
	assert(value == -1);
*/
}

#endif


void JSONTest::testStringProperty()
{
	std::string json = "{ \"test\" : \"value\" }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object object = *result.extract<Object::Ptr>();
	Var test = object.get("test");
	assert(test.isString());
	std::string value = test.convert<std::string>();
	assert(value.compare("value") == 0);

	DynamicStruct ds = object;
	assert (!ds["test"].isEmpty());
	assert (ds["test"].isString());
	assert (!ds["test"].isInteger());
	assert (ds["test"] == "value");
	value = ds["test"].toString();
	assert(value == "value");

	const DynamicStruct& rds = object;
	assert (!rds["test"].isEmpty());
	assert (rds["test"].isString());
	assert (!rds["test"].isInteger());
	assert (rds["test"] == "value");
	value = rds["test"].toString();
	assert(value == "value");
}


void JSONTest::testEmptyObject()
{
	std::string json = "{}";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	assert(object->size() == 0);

	DynamicStruct ds = *object;
	assert (ds.size() == 0);

	const DynamicStruct& rds = *object;
	assert (rds.size() == 0);
}


void JSONTest::testComplexObject()
{
	std::string json = 
	"{"
		"\"id\": 1,"
		"\"jsonrpc\": \"2.0\","
		"\"total\": 1,"
		"\"result\": ["
			"{"
				"\"id\": 1,"
				"\"guid\": \"67acfb26-17eb-4a75-bdbd-f0669b7d8f73\","
				"\"picture\": \"http://placehold.it/32x32\","
				"\"age\": 40,"
				"\"name\": \"Angelina Crossman\","
				"\"gender\": \"female\","
				"\"company\": \"Raylog\","
				"\"phone\": \"892-470-2803\","
				"\"email\": \"angelina@raylog.com\","
				"\"address\": \"20726, CostaMesa, Horatio Streets\","
				"\"about\": \"Consectetuer suscipit volutpat eros dolor euismod, "
				"et dignissim in feugiat sed, ea tation exerci quis. Consectetuer, "
				"dolor dolore ad vero ullamcorper, tincidunt facilisis at in facilisi, "
				"iusto illum illum. Autem nibh, sed elit consequat volutpat tation, "
				"nisl lorem lorem sed tation, facilisis dolore. Augue odio molestie, "
				"dolor zzril nostrud aliquam sed, wisi dolor et ut iusto, ea. Magna "
				"ex qui facilisi, hendrerit quis in eros ut, zzril nibh dolore nisl "
				"suscipit, vulputate elit ut lobortis exerci, nulla dolore eros at "
				"aliquam, ullamcorper vero ad iusto. Adipiscing, nisl eros exerci "
				"nisl vel, erat in luptatum in duis, iusto.\","
				"\"registered\": \"2008-04-09T11:13:17 +05:00\","
				"\"tags\": ["
					"\"ut\","
					"\"accumsan\","
					"\"feugait\","
					"\"ex\","
					"\"odio\","
					"\"consequat\","
					"\"delenit\""
				"],"
				"\"friends\": ["
					"{"
						"\"id\": 1,"
						"\"name\": \"Hailey Hardman\""
					"},"
					"{"
						"\"id\": 2,"
						"\"name\": \"Bailey Oldridge\""
					"},"
					"{"
						"\"id\": 3,"
						"\"name\": \"Makayla Campbell\""
					"}"
				"]"
			"}"
		"]"
	"}";
	
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	assert(object->size() > 0);

	DynamicStruct ds = *object;
	assert (ds.size() > 0);
	assert (ds["id"] == 1);
	assert (ds["jsonrpc"] == 2.0);

	assert (ds["result"].isArray());
	assert (ds["result"].size() == 1);
	assert (ds["result"][0].isStruct());
	assert (ds["result"][0]["id"] == 1);
	assert (ds["result"][0]["guid"] == "67acfb26-17eb-4a75-bdbd-f0669b7d8f73");
	assert (ds["result"][0]["age"] == 40);
	assert (ds["result"][0]["name"] == "Angelina Crossman");
	assert (ds["result"][0]["gender"] == "female");
	assert (ds["result"][0]["company"] == "Raylog");
	assert (ds["result"][0]["phone"] == "892-470-2803");
	assert (ds["result"][0]["email"] == "angelina@raylog.com");
	assert (ds["result"][0]["address"] == "20726, CostaMesa, Horatio Streets");
	assert (ds["result"][0]["about"] == "Consectetuer suscipit volutpat eros dolor euismod, "
		"et dignissim in feugiat sed, ea tation exerci quis. Consectetuer, "
		"dolor dolore ad vero ullamcorper, tincidunt facilisis at in facilisi, "
		"iusto illum illum. Autem nibh, sed elit consequat volutpat tation, "
		"nisl lorem lorem sed tation, facilisis dolore. Augue odio molestie, "
		"dolor zzril nostrud aliquam sed, wisi dolor et ut iusto, ea. Magna "
		"ex qui facilisi, hendrerit quis in eros ut, zzril nibh dolore nisl "
		"suscipit, vulputate elit ut lobortis exerci, nulla dolore eros at "
		"aliquam, ullamcorper vero ad iusto. Adipiscing, nisl eros exerci "
		"nisl vel, erat in luptatum in duis, iusto.");
	assert (ds["result"][0]["registered"] == "2008-04-09T11:13:17 +05:00");

	assert (ds["result"][0]["tags"].isArray());
	assert (ds["result"][0]["tags"].size() == 7);
	assert (ds["result"][0]["tags"][0] == "ut");
	assert (ds["result"][0]["tags"][1] == "accumsan");
	assert (ds["result"][0]["tags"][2] == "feugait");
	assert (ds["result"][0]["tags"][3] == "ex");
	assert (ds["result"][0]["tags"][4] == "odio");
	assert (ds["result"][0]["tags"][5] == "consequat");
	assert (ds["result"][0]["tags"][6] == "delenit");

	assert (ds["result"][0]["friends"][0].isStruct());
	assert (ds["result"][0]["friends"][0]["id"] == 1);
	assert (ds["result"][0]["friends"][0]["name"] == "Hailey Hardman");
	assert (ds["result"][0]["friends"][1]["id"] == 2);
	assert (ds["result"][0]["friends"][1]["name"] == "Bailey Oldridge");
	assert (ds["result"][0]["friends"][2]["id"] == 3);
	assert (ds["result"][0]["friends"][2]["name"] == "Makayla Campbell");

	const DynamicStruct& rds = *object;
	assert (rds.size() > 0);
}


void JSONTest::testDoubleProperty()
{
	std::string json = "{ \"test\" : 1234.5 }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.isNumeric());
	double value = test;
	assert(value == 1234.5);

	DynamicStruct ds = *object;
	assert (ds["test"] == 1234.5);
}


void JSONTest::testDouble2Property()
{
	std::string json = "{ \"test\" : 12e34 }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.isNumeric());
	double value = test;
	assert(value >= 1.99e34 && value <= 12.01e34);
}


void JSONTest::testDouble3Property()
{
	std::string json = "{ \"test\" : 12e-34 }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");
	assert(test.isNumeric());
	double value = test;
	assert(value == 12e-34);
}


void JSONTest::testObjectProperty()
{
	std::string json = "{ \"test\" : { \"property\" : \"value\" } }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));
	
	Object::Ptr object = result.extract<Object::Ptr>();
	assert (object->isObject("test"));
	assert (!object->isArray("test"));

	Var test = object->get("test");
	assert(test.type() == typeid(Object::Ptr));
	Object::Ptr subObject = test.extract<Object::Ptr>();

	test = subObject->get("property");
	assert(test.isString());
	std::string value = test.convert<std::string>();
	assert(value.compare("value") == 0);

	DynamicStruct ds = *object;
	assert (ds["test"].isStruct());
	assert (ds["test"]["property"] == "value");
}


void JSONTest::testObjectArray()
{
	std::string json = "{ \"test\" : { \"test1\" : [1, 2, 3], \"test2\" : 4 } }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));
	Object::Ptr object = result.extract<Object::Ptr>();
	assert(object->isObject("test"));
	Object::Ptr subObject = object->getObject("test");
	assert(!subObject->isObject("test1"));
	assert(subObject->isArray("test1"));
	assert(!subObject->isObject("test2"));
	assert(!subObject->isArray("test2"));

	DynamicStruct ds = *object;
	assert (ds.size() > 0);
	assert (ds.size() == 1);

	assert (ds["test"].isStruct());
	assert (ds["test"]["test1"].isArray());
	assert (ds["test"]["test1"].size() == 3);
	assert (ds["test"]["test1"][0] == 1);
	assert (ds["test"]["test1"][1] == 2);
	assert (ds["test"]["test1"][2] == 3);
	assert (ds["test"]["test2"] == 4);
}


void JSONTest::testArrayOfObjects()
{
	std::string json = "[ {\"test\" : 0}, { \"test1\" : [1, 2, 3], \"test2\" : 4 } ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));
	Poco::JSON::Array::Ptr arr = result.extract<Poco::JSON::Array::Ptr>();
	Object::Ptr object = arr->getObject(0);
	assert (object->getValue<int>("test") == 0);
	Object::Ptr subObject = arr->getObject(1);
	Poco::JSON::Array::Ptr subArr = subObject->getArray("test1");
	result = subArr->get(0);
	assert (result == 1);

	Poco::Dynamic::Array da = *arr;
	assert (da.size() == 2);
	assert (da[0].isStruct());
	assert (da[0]["test"] == 0);
	assert (da[1].isStruct());
	assert (da[1]["test1"].isArray());
	assert (da[1]["test1"][0] == 1);
	assert (da[1]["test1"][1] == 2);
	assert (da[1]["test1"][2] == 3);
	assert (da[1]["test2"] == 4);
}


void JSONTest::testEmptyArray()
{
	std::string json = "[]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	assert(array->size() == 0);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 0);
}


void JSONTest::testNestedArray()
{
	std::string json = "[[[[]]]]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	assert(array->size() == 1);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0].size() == 1);
	assert (da[0][0].size() == 1);
	assert (da[0][0][0].size() == 0);
}


void JSONTest::testNullElement()
{
	std::string json = "[ null ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	assert(array->isNull(0));
	Var test = array->get(0);
	assert(test.isEmpty());

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0].isEmpty());
}


void JSONTest::testTrueElement()
{
	std::string json = "[ true ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	Var test = array->get(0);
	assert(test.type() == typeid(bool));
	bool value = test;
	assert(value);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0]);
}


void JSONTest::testFalseElement()
{
	std::string json = "[ false ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	Var test = array->get(0);
	assert(test.type() == typeid(bool));
	bool value = test;
	assert(!value);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (!da[0]);
}


void JSONTest::testNumberElement()
{
	std::string json = "[ 1969 ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	Var test = array->get(0);
	assert(test.isInteger());
	int value = test;
	assert(value == 1969);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0] == 1969);
}


void JSONTest::testStringElement()
{
	std::string json = "[ \"value\" ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	Var test = array->get(0);
	assert(test.isString());
	std::string value = test.convert<std::string>();
	assert(value.compare("value") == 0);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0] == "value");
}


void JSONTest::testEmptyObjectElement()
{
	std::string json = "[{}]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	Object::Ptr object = array->getObject(0);
	assert(object->size() == 0);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0].isStruct());
	assert (da[0].size() == 0);
}


void JSONTest::testDoubleElement()
{
	std::string json = "[ 1234.5 ]";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();
	Var test = array->get(0);
	assert(test.isNumeric());
	double value = test;
	assert(value == 1234.5);

	Poco::Dynamic::Array da = *array;
	assert (da.size() == 1);
	assert (da[0] == 1234.5);
}


void JSONTest::testSetArrayElement()
{
	std::string json = "[]";
	Parser parser;
	Var result = parser.parse(json);
	Poco::JSON::Array::Ptr array = result.extract<Poco::JSON::Array::Ptr>();

	// array[0] = 7
	array->set(0, 7);
	assert(array->size() == 1);
	assert(array->getElement<int>(0) == 7);

	// array[2] = "foo"
	array->set(2, std::string("foo"));
	assert(array->size() == 3);
	assert(array->getElement<int>(0) == 7);
	assert(array->isNull(1));
	assert(array->getElement<std::string>(2) == "foo");

	// array[1] = 13
	array->set(1, 13);
	assert(array->size() == 3);
	assert(array->getElement<int>(0) == 7);
	assert(array->getElement<int>(1) == 13);
	assert(array->getElement<std::string>(2) == "foo");
}


void JSONTest::testOptValue()
{
	std::string json = "{ }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	int n = object->optValue("test", 123);
	assert(n == 123);
}


void JSONTest::testQuery()
{
	std::string json = "{ \"name\" : \"Franky\", \"children\" : [ \"Jonas\", \"Ellen\" ], \"address\": { \"street\": \"A Street\", \"number\": 123, \"city\":\"The City\"} }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Query query(result);

	std::string firstChild = query.findValue("children[0]", "");
	assert(firstChild.compare("Jonas") == 0);

	Poco::DynamicStruct ds = *result.extract<Object::Ptr>();
	assert (ds["name"] == "Franky");
	assert (ds["children"].size() == 2);
	assert (ds["children"][0] == "Jonas");
	assert (ds["children"][1] == "Ellen");

	Object::Ptr pAddress = query.findObject("address");
	assert (pAddress->getValue<std::string>("street") == "A Street");
	pAddress = query.findObject("bad address");
	assert (pAddress.isNull());

	Object address;
	address.set("dummy", 123);
	query.findObject("bad address", address);
	assert (!address.has("dummy"));
	Object& rAddress = query.findObject("address", address);
	assert (rAddress.getValue<int>("number") == 123);

	Var badAddr = query.find("address.street.anotherObject");
	assert(badAddr.isEmpty());

	using Poco::JSON::Array;

	Array::Ptr pChildren = query.findArray("children");
	assert (pChildren->getElement<std::string>(0) == "Jonas");
	pChildren = query.findArray("no children");
	assert (pChildren.isNull());

	Array children;
	children.add("dummy");
	query.findArray("no children", children);
	assert (children.size() == 0);
	Array& rChildren = query.findArray("children", children);
	assert (rChildren.getElement<std::string>(1) == "Ellen");

	Object::Ptr pObj = new Poco::JSON::Object;
	pObj->set("Id", 22);

	Query queryPointer(pObj);
	Var idQueryPointer = queryPointer.find("Id");
	assert(22 == idQueryPointer);

	Query queryObj(*pObj);
	Var idQueryObj = queryObj.find("Id");
	assert (22 == idQueryObj);

	Var bad = 1;
	try
	{
		Query badQuery(bad);
		fail ("must throw");
	}
	catch (Poco::InvalidArgumentException&) { }
}


void JSONTest::testComment()
{
	std::string json = "{ \"name\" : \"Franky\" /* father */, \"children\" : [ \"Jonas\" /* son */ , \"Ellen\" /* daughter */ ] }";
	Parser parser;
	Var result;

	try
	{
		parser.parse(json);
		fail ("must fail");
	}
	catch(Poco::SyntaxException&)
	{
	}
	
	parser.reset();
	parser.setAllowComments(true);
	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));

	Query query(result);

	std::string firstChild = query.findValue("children[0]", "");
	assert(firstChild.compare("Jonas") == 0);
}


void JSONTest::testPrintHandler()
{
	std::string json = "{ \"name\" : \"Homer\", \"age\" : 38, \"wife\" : \"Marge\", \"age\" : 36, \"children\" : [ \"Bart\", \"Lisa\", \"Maggie\" ] }";
	Parser parser;
	std::ostringstream ostr;
	PrintHandler::Ptr pHandler = new PrintHandler(ostr);
	parser.setHandler(pHandler);
	parser.parse(json);
	assert (ostr.str() == "{\"name\":\"Homer\",\"age\":38,\"wife\":\"Marge\",\"age\":36,\"children\":[\"Bart\",\"Lisa\",\"Maggie\"]}");

	pHandler->setIndent(1);
	ostr.str("");
	parser.reset();
	parser.parse(json);
	assert (ostr.str() == "{\n"
		" \"name\" : \"Homer\",\n"
		" \"age\" : 38,\n"
		" \"wife\" : \"Marge\",\n"
		" \"age\" : 36,\n"
		" \"children\" : [\n"
		"  \"Bart\",\n"
		"  \"Lisa\",\n"
		"  \"Maggie\"\n"
		" ]\n"
		"}"
	);

	pHandler->setIndent(2);
	ostr.str("");
	parser.reset();
	parser.parse(json);
	assert (ostr.str() == "{\n"
		"  \"name\" : \"Homer\",\n"
		"  \"age\" : 38,\n"
		"  \"wife\" : \"Marge\",\n"
		"  \"age\" : 36,\n"
		"  \"children\" : [\n"
		"    \"Bart\",\n"
		"    \"Lisa\",\n"
		"    \"Maggie\"\n"
		"  ]\n"
		"}"
	);

	pHandler->setIndent(4);
	ostr.str("");
	parser.reset();
	parser.parse(json);
	assert (ostr.str() == "{\n"
		"    \"name\" : \"Homer\",\n"
		"    \"age\" : 38,\n"
		"    \"wife\" : \"Marge\",\n"
		"    \"age\" : 36,\n"
		"    \"children\" : [\n"
		"        \"Bart\",\n"
		"        \"Lisa\",\n"
		"        \"Maggie\"\n"
		"    ]\n"
		"}"
	);

	json = 
		"{"
			"\"array\":"
			"["
				"{"
					"\"key1\":"
					"["
						"1,2,3,"
						"{"
							"\"subkey\":"
							"\"test\""
						"}"
					"]"
				"},"
				"{"
					"\"key2\":"
					"{"
						"\"anotherSubKey\":"
						"["
							"1,"
							"{"
								"\"subSubKey\":"
								"["
									"4,5,6"
								"]"
							"}"
						"]"
					"}"
				"}"
			"]"
		"}";


	ostr.str("");
	pHandler->setIndent(0);
	parser.reset();
	parser.parse(json);
	assert (json == ostr.str());

	json="[[\"a\"],[\"b\"],[[\"c\"],[\"d\"]]]";
	ostr.str("");
	pHandler->setIndent(0);
	parser.reset();
	parser.parse(json);
    std::cout << ostr.str() << std::endl;
	assert (json == ostr.str());

	json="[{\"1\":\"one\",\"0\":[\"zero\",\"nil\"]}]";
	ostr.str("");
	pHandler->setIndent(0);
	parser.reset();
	parser.parse(json);
	assert (json == ostr.str());

}


void JSONTest::testStringify()
{
	std::string str1 = "\r";
	std::string str2 = "\n";
	Poco::JSON::Object obj1, obj2;
	obj1.set("payload", str1);
	obj2.set("payload", str2);
	std::ostringstream oss1, oss2;
	Poco::JSON::Stringifier::stringify(obj1, oss1);
	Poco::JSON::Stringifier::stringify(obj2, oss2);
	assert(oss1.str() == "{\"payload\":\"\\r\"}");
	std::cout << "\"" << oss1.str() << "\"" << std::endl;
	assert(oss2.str() == "{\"payload\":\"\\n\"}");

	Object jObj(false);
	jObj.set("foo\\", 0);
	jObj.set("bar/", 0);
	jObj.set("baz", 0);
	jObj.set("q\"uote\"d", 0);
	jObj.set("backspace", "bs\b");
	jObj.set("newline", "nl\n");
	jObj.set("tab", "tb\t");

	std::stringstream ss;
	jObj.stringify(ss);

	assert(ss.str() == "{\"backspace\":\"bs\\b\",\"bar\\/\":0,\"baz\":0,\"foo\\\\\":0,"
		"\"newline\":\"nl\\n\",\"q\\\"uote\\\"d\":0,\"tab\":\"tb\\t\"}");

	std::string json = "{ \"Simpsons\" : { \"husband\" : { \"name\" : \"Homer\" , \"age\" : 38 }, \"wife\" : { \"name\" : \"Marge\", \"age\" : 36 }, "
						"\"children\" : [ \"Bart\", \"Lisa\", \"Maggie\" ], "
						"\"address\" : { \"number\" : 742, \"street\" : \"Evergreen Terrace\", \"town\" : \"Springfield\" } } }";
	Parser parser;
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));
	std::ostringstream ostr;

	Stringifier::condense(result, ostr);
	std::string str = "{"
						"\"Simpsons\":{"
						"\"address\":{"
						"\"number\":742,"
						"\"street\":\"Evergreen Terrace\","
						"\"town\":\"Springfield\""
						"},"
						"\"children\":["
						"\"Bart\","
						"\"Lisa\","
						"\"Maggie\"],"
						"\"husband\":{"
						"\"age\":38,"
						"\"name\":\"Homer\""
						"},"
						"\"wife\":{"
						"\"age\":36,\"name\":\"Marge\""
						"}}}";

	assert (ostr.str() == str);

	ostr.str("");
	Stringifier::stringify(result, ostr);
	assert (ostr.str() == str);

	ostr.str("");
	Stringifier::stringify(result, ostr, 1);
	str = "{\n"
		" \"Simpsons\" : {\n"
		"  \"address\" : {\n"
		"   \"number\" : 742,\n"
		"   \"street\" : \"Evergreen Terrace\",\n"
		"   \"town\" : \"Springfield\"\n"
		"  },\n"
		"  \"children\" : [\n"
		"   \"Bart\",\n"
		"   \"Lisa\",\n"
		"   \"Maggie\"\n"
		"  ],\n"
		"  \"husband\" : {\n"
		"   \"age\" : 38,\n"
		"   \"name\" : \"Homer\"\n"
		"  },\n"
		"  \"wife\" : {\n"
		"   \"age\" : 36,\n"
		"   \"name\" : \"Marge\"\n"
		"  }\n"
		" }\n"
		"}";
	assert (ostr.str() == str);

	ostr.str("");
	Stringifier::stringify(result, ostr, 2);
	str = "{\n"
		"  \"Simpsons\" : {\n"
		"    \"address\" : {\n"
		"      \"number\" : 742,\n"
		"      \"street\" : \"Evergreen Terrace\",\n"
		"      \"town\" : \"Springfield\"\n"
		"    },\n"
		"    \"children\" : [\n"
		"      \"Bart\",\n"
		"      \"Lisa\",\n"
		"      \"Maggie\"\n"
		"    ],\n"
		"    \"husband\" : {\n"
		"      \"age\" : 38,\n"
		"      \"name\" : \"Homer\"\n"
		"    },\n"
		"    \"wife\" : {\n"
		"      \"age\" : 36,\n"
		"      \"name\" : \"Marge\"\n"
		"    }\n"
		"  }\n"
		"}";
	assert (ostr.str() == str);

	ostr.str("");
	Stringifier::stringify(result, ostr, 4);
	str = "{\n"
		"    \"Simpsons\" : {\n"
		"        \"address\" : {\n"
		"            \"number\" : 742,\n"
		"            \"street\" : \"Evergreen Terrace\",\n"
		"            \"town\" : \"Springfield\"\n"
		"        },\n"
		"        \"children\" : [\n"
		"            \"Bart\",\n"
		"            \"Lisa\",\n"
		"            \"Maggie\"\n"
		"        ],\n"
		"        \"husband\" : {\n"
		"            \"age\" : 38,\n"
		"            \"name\" : \"Homer\"\n"
		"        },\n"
		"        \"wife\" : {\n"
		"            \"age\" : 36,\n"
		"            \"name\" : \"Marge\"\n"
		"        }\n"
		"    }\n"
		"}";
	assert (ostr.str() == str);
}


void JSONTest::testStringifyPreserveOrder()
{
	Object presObj(true);
	presObj.set("foo", 0);
	presObj.set("bar", 0);
	presObj.set("baz", 0);
	std::stringstream ss;
	presObj.stringify(ss);
	assert(ss.str() == "{\"foo\":0,\"bar\":0,\"baz\":0}");
	ss.str("");
	Stringifier::stringify(presObj, ss);
	assert(ss.str() == "{\"foo\":0,\"bar\":0,\"baz\":0}");

	Object noPresObj;
	noPresObj.set("foo", 0);
	noPresObj.set("bar", 0);
	noPresObj.set("baz", 0);
	ss.str("");
	noPresObj.stringify(ss);
	assert(ss.str() == "{\"bar\":0,\"baz\":0,\"foo\":0}");
	ss.str("");
	Stringifier::stringify(noPresObj, ss);
	assert(ss.str() == "{\"bar\":0,\"baz\":0,\"foo\":0}");

	std::string json = "{ \"Simpsons\" : { \"husband\" : { \"name\" : \"Homer\" , \"age\" : 38 }, \"wife\" : { \"name\" : \"Marge\", \"age\" : 36 }, "
						"\"children\" : [ \"Bart\", \"Lisa\", \"Maggie\" ], "
						"\"address\" : { \"number\" : 742, \"street\" : \"Evergreen Terrace\", \"town\" : \"Springfield\" } } }";

	ParseHandler::Ptr pHandler = new ParseHandler(true);
	Parser parser(pHandler);
	Var result;

	try
	{
		result = parser.parse(json);
	}
	catch(JSONException& jsone)
	{
		std::cout << jsone.message() << std::endl;
		assert(false);
	}

	assert(result.type() == typeid(Object::Ptr));
	std::ostringstream ostr;

	Stringifier::condense(result, ostr);
	std::cout << ostr.str() << std::endl;
	assert (ostr.str() == "{\"Simpsons\":{\"husband\":{\"name\":\"Homer\",\"age\":38},\"wife\":{\"name\":\"Marge\",\"age\":36},"
						"\"children\":[\"Bart\",\"Lisa\",\"Maggie\"],"
						"\"address\":{\"number\":742,\"street\":\"Evergreen Terrace\",\"town\":\"Springfield\"}}}");

	ostr.str("");
	Stringifier::stringify(result, ostr);
	assert (ostr.str() == "{\"Simpsons\":{\"husband\":{\"name\":\"Homer\",\"age\":38},\"wife\":{\"name\":\"Marge\",\"age\":36},"
						"\"children\":[\"Bart\",\"Lisa\",\"Maggie\"],"
						"\"address\":{\"number\":742,\"street\":\"Evergreen Terrace\",\"town\":\"Springfield\"}}}");
	
	ostr.str("");
	Stringifier::stringify(result, ostr, 1);
	assert (ostr.str() == "{\n"
						" \"Simpsons\" : {\n"
						"  \"husband\" : {\n"
						"   \"name\" : \"Homer\",\n"
						"   \"age\" : 38\n"
						"  },\n"
						"  \"wife\" : {\n"
						"   \"name\" : \"Marge\",\n"
						"   \"age\" : 36\n"
						"  },\n"
						"  \"children\" : [\n"
						"   \"Bart\",\n"
						"   \"Lisa\",\n"
						"   \"Maggie\"\n"
						"  ],\n"
						"  \"address\" : {\n"
						"   \"number\" : 742,\n"
						"   \"street\" : \"Evergreen Terrace\",\n" 
						"   \"town\" : \"Springfield\"\n"
						"  }\n"
						" }\n"
						"}");

	ostr.str("");
	Stringifier::stringify(result, ostr, 2);
	assert (ostr.str() == "{\n"
						"  \"Simpsons\" : {\n"
						"    \"husband\" : {\n"
						"      \"name\" : \"Homer\",\n"
						"      \"age\" : 38\n"
						"    },\n"
						"    \"wife\" : {\n"
						"      \"name\" : \"Marge\",\n"
						"      \"age\" : 36\n"
						"    },\n"
						"    \"children\" : [\n"
						"      \"Bart\",\n"
						"      \"Lisa\",\n"
						"      \"Maggie\"\n"
						"    ],\n"
						"    \"address\" : {\n"
						"      \"number\" : 742,\n"
						"      \"street\" : \"Evergreen Terrace\",\n" 
						"      \"town\" : \"Springfield\"\n"
						"    }\n"
						"  }\n"
						"}");

	ostr.str("");
	Stringifier::stringify(result, ostr, 4);
	assert (ostr.str() == "{\n"
						"    \"Simpsons\" : {\n"
						"        \"husband\" : {\n"
						"            \"name\" : \"Homer\",\n"
						"            \"age\" : 38\n"
						"        },\n"
						"        \"wife\" : {\n"
						"            \"name\" : \"Marge\",\n"
						"            \"age\" : 36\n"
						"        },\n"
						"        \"children\" : [\n"
						"            \"Bart\",\n"
						"            \"Lisa\",\n"
						"            \"Maggie\"\n"
						"        ],\n"
						"        \"address\" : {\n"
						"            \"number\" : 742,\n"
						"            \"street\" : \"Evergreen Terrace\",\n" 
						"            \"town\" : \"Springfield\"\n"
						"        }\n"
						"    }\n"
						"}");

	Poco::DynamicStruct ds = *result.extract<Object::Ptr>();
	assert (ds["Simpsons"].isStruct());
	assert (ds["Simpsons"]["husband"].isStruct());
	assert (ds["Simpsons"]["husband"]["name"] == "Homer");
	assert (ds["Simpsons"]["husband"]["age"] == 38);
	
	assert (ds["Simpsons"]["wife"].isStruct());
	assert (ds["Simpsons"]["wife"]["name"] == "Marge");
	assert (ds["Simpsons"]["wife"]["age"] == 36);
	
	assert (ds["Simpsons"]["children"].isArray());
	assert (ds["Simpsons"]["children"][0] == "Bart");
	assert (ds["Simpsons"]["children"][1] == "Lisa");
	assert (ds["Simpsons"]["children"][2] == "Maggie");
	
	assert (ds["Simpsons"]["address"].isStruct());
	assert (ds["Simpsons"]["address"]["number"] == 742);
	assert (ds["Simpsons"]["address"]["street"] == "Evergreen Terrace");
	assert (ds["Simpsons"]["address"]["town"] == "Springfield");
}


void JSONTest::testValidJanssonFiles()
{
	Poco::Path pathPattern(getTestFilesPath("valid"));

	std::set<std::string> paths;
	Poco::Glob::glob(pathPattern, paths);

	for(std::set<std::string>::iterator it = paths.begin(); it != paths.end(); ++it)
	{
		Poco::Path filePath(*it, "input");

		if ( filePath.isFile() )
		{
			Poco::File inputFile(filePath);
			if ( inputFile.exists() )
			{
				Poco::FileInputStream fis(filePath.toString());
				std::cout << filePath.toString() << std::endl;

				Parser parser;
				Var result;

				try
				{
					parser.parse(fis);
					result = parser.asVar();
					std::cout << "Ok!" << std::endl;
				}
				catch(JSONException& jsone)
				{
					std::string err = jsone.displayText();
					std::cout << "Failed:" << err << std::endl;
					fail (err);
				}
				catch(Poco::Exception& e)
				{
					std::string err = e.displayText();
					std::cout << "Failed:" << err << std::endl;
					fail (err);
				}
			}
		}
	}
}


void JSONTest::testInvalidJanssonFiles()
{
	Poco::Path pathPattern(getTestFilesPath("invalid"));

	std::set<std::string> paths;
	Poco::Glob::glob(pathPattern, paths);

	for(std::set<std::string>::iterator it = paths.begin(); it != paths.end(); ++it)
	{
		Poco::Path filePath(*it, "input");

		if ( filePath.isFile() )
		{
			Poco::File inputFile(filePath);
			if ( inputFile.exists() )
			{
				Poco::FileInputStream fis(filePath.toString());
				std::cout << filePath.toString() << std::endl;

				Parser parser;
				parser.setAllowNullByte(false);
				Var result;

				try
				{
					parser.parse(fis);
					result = parser.asVar();
					// We shouldn't get here.
					std::cout << "We didn't get an exception. This is the result: " << result.convert<std::string>() << std::endl; 
					fail(result.convert<std::string>());
				}
				catch(JSONException&)
				{
					continue;
				}
				catch(Poco::SyntaxException&)
				{ }
			}
		}
	}
}


void JSONTest::testInvalidUnicodeJanssonFiles()
{
	Poco::Path pathPattern(getTestFilesPath("invalid-unicode"));

	std::set<std::string> paths;
	Poco::Glob::glob(pathPattern, paths);

	for(std::set<std::string>::iterator it = paths.begin(); it != paths.end(); ++it)
	{
		Poco::Path filePath(*it, "input");

		if ( filePath.isFile() )
		{
			Poco::File inputFile(filePath);
			if ( inputFile.exists() )
			{
				Poco::FileInputStream fis(filePath.toString());
				std::cout << filePath.toString() << std::endl;

				Parser parser;
				parser.setAllowNullByte(false);
				Var result;

				try
				{
					parser.parse(fis);
					result = parser.asVar();
					// We shouldn't get here.
					std::cout << "We didn't get an exception. This is the result: " << result.convert<std::string>() << std::endl; 
					fail(result.convert<std::string>());
				}
				catch(JSONException&)
				{
					continue;
				}
				catch(Poco::SyntaxException&)
				{ }
			}
		}
	}
}


void JSONTest::testTemplate()
{
	Template tpl;
	tpl.parse("Hello world! From <?= person.name?>.\n<?if person.tooOld?>You're too old.<?endif?>");

	Object::Ptr data = new Object();
	Object::Ptr person = new Object();
	data->set("person", person);
	person->set("name", "Franky");
	person->set("tooOld", true);
	std::ostringstream ostr;
	tpl.render(data, ostr);
	std::cout << ostr.str();
	assert (ostr.str() == "Hello world! From Franky.\nYou're too old.");
}


void JSONTest::testUnicode()
{
	const unsigned char supp[] = {0x61, 0xE1, 0xE9, 0x78, 0xED, 0xF3, 0xFA, 0x0};
	std::string text((const char*) supp);

	std::string json = "{ \"test\" : \"a\\u00E1\\u00E9x\\u00ED\\u00F3\\u00FA\" }";
	Parser parser;

	Var result;
	parser.parse(json);
	result = parser.asVar();

	assert(result.type() == typeid(Object::Ptr));

	Object::Ptr object = result.extract<Object::Ptr>();
	Var test = object->get("test");

	Poco::Latin1Encoding latin1;
	Poco::UTF8Encoding utf8;
	Poco::TextConverter converter(latin1, utf8);
	std::string original;
	converter.convert(text, original);

	assert(test.convert<std::string>() == original);

	parser.reset();
	std::ostringstream os;
	os << '[' << (char) 0x92 << ']';
	try
	{
		parser.parse(os.str());
		fail("Invalid Unicode sequence, must fail.");
	}
	catch (JSONException&) {}

	parser.reset();
	os.str("");
	os << '[' << (char)0xC2 << (char)0x92 << ']';
	result = parser.parse(os.str());
	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	parser.reset();
	os.str("");
	os << '[' << (char)0xAC << ']';
	try
	{
		parser.parse(os.str());
		fail("Invalid Unicode sequence, must fail.");
	}
	catch (JSONException&) {}

	parser.reset();
	os.str("");
	os << '[' << (char)0xE2 << (char)0x82 << (char)0xAC << ']';
	result = parser.parse(os.str());
	assert(result.type() == typeid(Poco::JSON::Array::Ptr));

	parser.reset();
	os.str("");
	os << '[' << (char)0xA2 << ']';
	try
	{
		parser.parse(os.str());
		fail("Invalid Unicode sequence, must fail.");
	}
	catch (JSONException&){}

	parser.reset();
	os.str("");
	os << '[' << (char)0xF0 << (char)0xA4 << (char)0xAD << (char)0xAD << ']';
	result = parser.parse(os.str());
	assert(result.type() == typeid(Poco::JSON::Array::Ptr));
}


void JSONTest::testSmallBuffer()
{
	Poco::JSON::Parser parser(new Poco::JSON::ParseHandler(), 4);
	std::string jsonStr = "{ \"x\" : \"123456789012345678901234567890123456789012345678901234567890\" }";
	parser.parse(jsonStr);
}


std::string JSONTest::getTestFilesPath(const std::string& type)
{
	std::ostringstream ostr;
	ostr << "data/" << type << '/';
	std::string validDir(ostr.str());
	Poco::Path pathPattern(validDir);
	if (Poco::File(pathPattern).exists())
	{
		validDir += '*';
		return validDir;
	}

	ostr.str("");
	ostr << "/JSON/testsuite/data/" << type << '/';
	validDir = Poco::Environment::get("POCO_BASE") + ostr.str();
	pathPattern = validDir;

	if (Poco::File(pathPattern).exists())
		validDir += '*';
	else
	{
		std::cout << "Can't find " << validDir << std::endl;
		throw Poco::NotFoundException("cannot locate directory containing valid JSON test files");
	}
	return validDir;
}


CppUnit::Test* JSONTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("JSONTest");

	CppUnit_addTest(pSuite, JSONTest, testNullProperty);
	CppUnit_addTest(pSuite, JSONTest, testTrueProperty);
	CppUnit_addTest(pSuite, JSONTest, testFalseProperty);
	CppUnit_addTest(pSuite, JSONTest, testNumberProperty);
	CppUnit_addTest(pSuite, JSONTest, testUnsignedNumberProperty);
#if defined(POCO_HAVE_INT64)
	CppUnit_addTest(pSuite, JSONTest, testNumber64Property);
	CppUnit_addTest(pSuite, JSONTest, testUnsignedNumber64Property);
#endif
	CppUnit_addTest(pSuite, JSONTest, testStringProperty);
	CppUnit_addTest(pSuite, JSONTest, testEmptyObject);
	CppUnit_addTest(pSuite, JSONTest, testComplexObject);
	CppUnit_addTest(pSuite, JSONTest, testDoubleProperty);
	CppUnit_addTest(pSuite, JSONTest, testDouble2Property);
	CppUnit_addTest(pSuite, JSONTest, testDouble3Property);
	CppUnit_addTest(pSuite, JSONTest, testObjectProperty);
	CppUnit_addTest(pSuite, JSONTest, testObjectArray);
	CppUnit_addTest(pSuite, JSONTest, testArrayOfObjects);
	CppUnit_addTest(pSuite, JSONTest, testEmptyArray);
	CppUnit_addTest(pSuite, JSONTest, testNestedArray);
	CppUnit_addTest(pSuite, JSONTest, testNullElement);
	CppUnit_addTest(pSuite, JSONTest, testTrueElement);
	CppUnit_addTest(pSuite, JSONTest, testFalseElement);
	CppUnit_addTest(pSuite, JSONTest, testNumberElement);
	CppUnit_addTest(pSuite, JSONTest, testStringElement);
	CppUnit_addTest(pSuite, JSONTest, testEmptyObjectElement);
	CppUnit_addTest(pSuite, JSONTest, testDoubleElement);
	CppUnit_addTest(pSuite, JSONTest, testSetArrayElement);
	CppUnit_addTest(pSuite, JSONTest, testOptValue);
	CppUnit_addTest(pSuite, JSONTest, testQuery);
	CppUnit_addTest(pSuite, JSONTest, testComment);
	CppUnit_addTest(pSuite, JSONTest, testPrintHandler);
	CppUnit_addTest(pSuite, JSONTest, testStringify);
	CppUnit_addTest(pSuite, JSONTest, testStringifyPreserveOrder);
	CppUnit_addTest(pSuite, JSONTest, testValidJanssonFiles);
	CppUnit_addTest(pSuite, JSONTest, testInvalidJanssonFiles);
	CppUnit_addTest(pSuite, JSONTest, testInvalidUnicodeJanssonFiles);
	CppUnit_addTest(pSuite, JSONTest, testTemplate);
	CppUnit_addTest(pSuite, JSONTest, testUnicode);
	CppUnit_addTest(pSuite, JSONTest, testSmallBuffer);

	return pSuite;
}
