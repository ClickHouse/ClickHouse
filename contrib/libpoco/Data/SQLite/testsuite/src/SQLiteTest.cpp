//
// SQLiteTest.cpp
//
// $Id: //poco/Main/Data/SQLite/testsuite/src/SQLiteTest.cpp#7 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SQLiteTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/Statement.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/SQLChannel.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/SQLite/Notifier.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Data/TypeHandler.h"
#include "Poco/Nullable.h"
#include "Poco/Data/Transaction.h"
#include "Poco/Data/DataException.h"
#include "Poco/Data/SQLite/SQLiteException.h"
#include "Poco/Tuple.h"
#include "Poco/Any.h"
#include "Poco/SharedPtr.h"
#include "Poco/DynamicAny.h"
#include "Poco/DateTime.h"
#include "Poco/Logger.h"
#include "Poco/Message.h"
#include "Poco/Thread.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Stopwatch.h"
#include "Poco/Delegate.h"
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;
using Poco::Data::RecordSet;
using Poco::Data::Column;
using Poco::Data::Row;
using Poco::Data::SQLChannel;
using Poco::Data::LimitException;
using Poco::Data::CLOB;
using Poco::Data::Date;
using Poco::Data::Time;
using Poco::Data::Transaction;
using Poco::Data::AbstractExtractionVec;
using Poco::Data::AbstractExtractionVecVec;
using Poco::Data::AbstractBindingVec;
using Poco::Data::NotConnectedException;
using Poco::Data::SQLite::Notifier;
using Poco::Nullable;
using Poco::Tuple;
using Poco::Any;
using Poco::AnyCast;
using Poco::DynamicAny;
using Poco::DateTime;
using Poco::Logger;
using Poco::Message;
using Poco::AutoPtr;
using Poco::Thread;
using Poco::format;
using Poco::InvalidAccessException;
using Poco::RangeException;
using Poco::BadCastException;
using Poco::NotFoundException;
using Poco::NullPointerException;
using Poco::TimeoutException;
using Poco::NotImplementedException;
using Poco::Data::SQLite::ConstraintViolationException;
using Poco::Data::SQLite::ParameterCountMismatchException;
using Poco::Int32;
using Poco::Dynamic::Var;
using Poco::Data::SQLite::Utility;
using Poco::delegate;


class Person
{
public:
	Person(){_age = 0;}
	Person(const std::string& ln, const std::string& fn, const std::string& adr, int a):_lastName(ln), _firstName(fn), _address(adr), _age(a)
	{
	}
	bool operator==(const Person& other) const
	{
		return _lastName == other._lastName && _firstName == other._firstName && _address == other._address && _age == other._age;
	}

	bool operator < (const Person& p) const
	{
		if (_age < p._age)
			return true;
		if (_lastName < p._lastName)
			return true;
		if (_firstName < p._firstName)
			return true;
		return (_address < p._address);
	}

	const std::string& operator () () const
		/// This method is required so we can extract data to a map!
	{
		// we choose the lastName as examplary key
		return _lastName;
	}

	const std::string& getLastName() const
	{
		return _lastName;
	}

	void setLastName(const std::string& lastName)
	{
		_lastName = lastName;
	}

	const std::string& getFirstName() const
	{
		return _firstName;
	}

	void setFirstName(const std::string& firstName)
	{
		_firstName = firstName;
	}

	const std::string& getAddress() const
	{
		return _address;
	}

	void setAddress(const std::string& address)
	{
		_address = address;
	}

	const int& getAge() const
	{
		return _age;
	}

	void setAge(const int& age)
	{
		_age = age;
	}

private:
	std::string _lastName;
	std::string _firstName;
	std::string _address;
	int         _age;
};


namespace Poco {
namespace Data {


template <>
class TypeHandler<Person>
{
public:
	static void bind(std::size_t pos, const Person& obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
	{
		// the table is defined as Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))
		poco_assert_dbg (!pBinder.isNull());
		pBinder->bind(pos++, obj.getLastName(), dir);
		pBinder->bind(pos++, obj.getFirstName(), dir);
		pBinder->bind(pos++, obj.getAddress(), dir);
		pBinder->bind(pos++, obj.getAge(), dir);
	}

	static void prepare(std::size_t pos, const Person& obj, AbstractPreparator::Ptr pPrepare)
	{
		// no-op (SQLite is prepare-less connector)
	}

	static std::size_t size()
	{
		return 4;
	}

	static void extract(std::size_t pos, Person& obj, const Person& defVal, AbstractExtractor::Ptr pExt)
	{
		poco_assert_dbg (!pExt.isNull());
		std::string lastName;
		std::string firstName;
		std::string address;
		int age;

		if (pExt->extract(pos++, lastName))
			obj.setLastName(lastName);
		else
			obj.setLastName(defVal.getLastName());

		if (pExt->extract(pos++, firstName))
			obj.setFirstName(firstName);
		else
			obj.setFirstName(defVal.getFirstName());

		if (pExt->extract(pos++, address))
			obj.setAddress(address);
		else
			obj.setAddress(defVal.getAddress());

		if (pExt->extract(pos++, age))
			obj.setAge(age);
		else
			obj.setAge(defVal.getAge());
	}

private:
	TypeHandler();
	~TypeHandler();
	TypeHandler(const TypeHandler&);
	TypeHandler& operator=(const TypeHandler&);
};


} } // namespace Poco::Data


int SQLiteTest::_insertCounter;
int SQLiteTest::_updateCounter;
int SQLiteTest::_deleteCounter;


SQLiteTest::SQLiteTest(const std::string& name): CppUnit::TestCase(name)
{
}


SQLiteTest::~SQLiteTest()
{
}


void SQLiteTest::testBinding()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	std::string tableName("Simpsons");
	std::string lastName("Simpson");
	std::string firstName("Bart");
	std::string address("Springfield");
	int age = 12;
	
	std::string& rLastName(lastName);
	std::string& rFirstName(firstName);
	std::string& rAddress(address);
	int& rAge = age;

	const std::string& crLastName(lastName);
	const std::string& crFirstName(firstName);
	const std::string& crAddress(address);
	const int& crAge = age;

	int count = 0;
	std::string result;

	tmp << "DROP TABLE IF EXISTS Simpsons", now;
	tmp << "CREATE TABLE IF NOT EXISTS Simpsons (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "SELECT name FROM sqlite_master WHERE tbl_name=?", use(tableName), into(result), now;
	assert (result == tableName);

	// following should not compile:
	//tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", use("Simpson"), use("Bart"), use("Springfield"), use(age), now;
	//tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", use(lastName), use(firstName), use(address), use(12), now;
	//tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", useRef(lastName), useRef(firstName), useRef(address), useRef(12), now;

	tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", useRef("Simpson"), useRef("Bart"), useRef("Springfield"), useRef(age), now;

	tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", use(rLastName), use(rFirstName), use(rAddress), use(rAge), now;
	tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", useRef(crLastName), useRef(crFirstName), useRef(crAddress), useRef(crAge), now;
	tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", bind("Simpson"), bind("Bart"), bind("Springfield"), bind(12), now;
	tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", bind(rLastName), bind(rFirstName), bind(rAddress), bind(rAge), now;
	tmp << "INSERT INTO Simpsons VALUES(?, ?, ?, ?)", bind(crLastName), bind(crFirstName), bind(crAddress), bind(crAge), now;

	tmp << "SELECT COUNT(*) FROM Simpsons", into(count), now;
	assert (6 == count);
}


void SQLiteTest::testZeroRows()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS ZeroTest", now;
	tmp << "CREATE TABLE IF NOT EXISTS ZeroTest (zt INTEGER(3))", now;
	Statement stmt = (tmp << "SELECT * FROM ZeroTest");
	assert(0 == stmt.execute());
}


void SQLiteTest::testSimpleAccess()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "SELECT name FROM sqlite_master WHERE tbl_name=?", use(tableName), into(result), now;
	assert (result == tableName);

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	tmp << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	tmp << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
	tmp << "UPDATE PERSON SET Age = -1", now;
	tmp << "SELECT Age FROM PERSON", into(age), now;
	assert (-1 == age);
	tmp.close();
	assert (!tmp.isConnected());
}


void SQLiteTest::testInMemory()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "SELECT name FROM sqlite_master WHERE tbl_name=?", use(tableName), into(result), now;
	assert (result == tableName);

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	
	// load db from file to memory
	Session mem (Poco::Data::SQLite::Connector::KEY, ":memory:");
	assert (Poco::Data::SQLite::Utility::fileToMemory(mem, "dummy.db"));

	mem << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	mem << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	mem << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
	mem << "UPDATE PERSON SET Age = -1", now;
	mem << "SELECT Age FROM PERSON", into(age), now;
	assert (-1 == age);
	
	// save db from memory to file on the disk
	Session dsk (Poco::Data::SQLite::Connector::KEY, "dsk.db");
	assert (Poco::Data::SQLite::Utility::memoryToFile("dsk.db", mem));

	dsk << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	dsk << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	dsk << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
	dsk << "UPDATE PERSON SET Age = -1", now;
	dsk << "SELECT Age FROM PERSON", into(age), now;
	assert (-1 == age);

	tmp.close();
	mem.close();
	dsk.close();

	assert (!tmp.isConnected());
	assert (!mem.isConnected());
	assert (!dsk.isConnected());
}


void SQLiteTest::testNullCharPointer()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::string lastName("lastname");
	int age = 100;
	int count = 100;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", 
		bind(lastName), 
		bind("firstname"), 
		bind("Address"), 
		bind(0), now;

	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	tmp << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	tmp << "SELECT Age FROM PERSON", into(age), now;
	assert (0 == age);

	try
	{
		const char* pc = 0;
		tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)",
			bind("lastname"), 
			bind("firstname"), 
			bind("Address"), bind(pc), now;
			fail ("must fail");
	} catch (NullPointerException&)	{ }

	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	tmp << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	tmp << "SELECT Age FROM PERSON", into(age), now;
	assert (0 == age);
}


void SQLiteTest::testInsertCharPointer()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	const char* pc = 0;
	try
	{
		tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", bind(pc), now;
		fail ("must fail");
	} catch (NullPointerException&)	{ }

	pc = (const char*) std::calloc(9, sizeof(char));
	poco_check_ptr (pc);
	std::strncpy((char*) pc, "lastname", 8);
	Statement stmt = (tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", 
		bind(pc), 
		bind("firstname"), 
		bind("Address"), 
		bind(133132));
	
	std::free((void*) pc); pc = 0;
	assert (1 == stmt.execute());

	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	tmp << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	tmp << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
}


void SQLiteTest::testInsertCharPointer2()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", 
		bind("lastname"), 
		bind("firstname"), 
		bind("Address"), 
		bind(133132), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	Statement stmt1 = (tmp << "SELECT LastName FROM PERSON", into(result));
	stmt1.execute();
	assert (lastName == result);
	count = 0;
	Statement stmt2 = (tmp << "SELECT Age FROM PERSON", into(count));
	stmt2.execute();
	assert (count == age);
}


void SQLiteTest::testComplexType()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(p1), now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(p2), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	Person c1;
	Person c2;
	tmp << "SELECT * FROM PERSON WHERE LASTNAME = :ln", into(c1), useRef(p1.getLastName()), now;
	assert (c1 == p1);

	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName1 VARCHAR(30), FirstName1 VARCHAR, Address1 VARCHAR, Age1 INTEGER(3),"
			"LastName2 VARCHAR(30), FirstName2 VARCHAR, Address2 VARCHAR, Age2 INTEGER(3))", now;
	
	Tuple<Person,Person> t(p1,p2);

	tmp << "INSERT INTO PERSON VALUES(:ln1, :fn1, :ad1, :age1, :ln2, :fn2, :ad2, :age2)", use(t), now;

	Tuple<Person,Person> ret;
	assert (ret != t);
	tmp << "SELECT * FROM PERSON", into(ret), now;
	assert (ret == t);
}



void SQLiteTest::testSimpleAccessVector()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<std::string> lastNames;
	std::vector<std::string> firstNames;
	std::vector<std::string> addresses;
	std::vector<int> ages;
	std::string tableName("Person");
	lastNames.push_back("LN1");
	lastNames.push_back("LN2");
	firstNames.push_back("FN1");
	firstNames.push_back("FN2");
	addresses.push_back("ADDR1");
	addresses.push_back("ADDR2");
	ages.push_back(1);
	ages.push_back(2);
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastNames), use(firstNames), use(addresses), use(ages), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::vector<std::string> lastNamesR;
	std::vector<std::string> firstNamesR;
	std::vector<std::string> addressesR;
	std::vector<int> agesR;
	tmp << "SELECT * FROM PERSON", into(lastNamesR), into(firstNamesR), into(addressesR), into(agesR), now;
	assert (ages == agesR);
	assert (lastNames == lastNamesR);
	assert (firstNames == firstNamesR);
	assert (addresses == addressesR);
}


void SQLiteTest::testComplexTypeVector()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<Person> people;
	people.push_back(Person("LN1", "FN1", "ADDR1", 1));
	people.push_back(Person("LN2", "FN2", "ADDR2", 2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::vector<Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result == people);
}


void SQLiteTest::testSharedPtrComplexTypeVector()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<Poco::SharedPtr<Person> > people;
	people.push_back(new Person("LN1", "FN1", "ADDR1", 1));
	people.push_back(new Person("LN2", "FN2", "ADDR2", 2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::vector<Poco::SharedPtr<Person> > result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (*result[0] == *people[0]);
	assert (*result[1] == *people[1]);
}


void SQLiteTest::testInsertVector()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<std::string> str;
	str.push_back("s1");
	str.push_back("s2");
	str.push_back("s3");
	str.push_back("s3");
	int count = 100;
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str VARCHAR(30))", now;
	{
		Statement stmt((tmp << "INSERT INTO Strings VALUES(:str)", use(str)));
		tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
		assert (count == 0);
		stmt.execute();
		tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
		assert (count == 4);
	}
	count = 0;
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 4);
}


void SQLiteTest::testInsertEmptyVector()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<std::string> str;

	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str VARCHAR(30))", now;
	try
	{
		tmp << "INSERT INTO Strings VALUES(:str)", use(str), now;
		fail("empty collectons should not work");
	}
	catch (Poco::Exception&)
	{
	}
}


void SQLiteTest::testAffectedRows()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<std::string> str;
	str.push_back("s1");
	str.push_back("s2");
	str.push_back("s3");
	str.push_back("s3");
	int count = 100;
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str VARCHAR(30))", now;

	Statement stmt((tmp << "INSERT INTO Strings VALUES(:str)", use(str)));
	count  = -1;
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 0);
	assert (4 == stmt.execute());
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 4);

	Statement stmt0(tmp << "DELETE FROM Strings");
	assert (4 == stmt0.execute());
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 0);

	Statement stmt1((tmp << "SELECT * FROM Strings"));
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 0);
	assert (0 == stmt1.execute());

	Statement stmt2((tmp << "INSERT INTO Strings VALUES(:str)", use(str)));
	count  = -1;
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 0);
	assert (4 == stmt2.execute());
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 4);

	Statement stmt3(tmp << "UPDATE Strings SET str = 's4' WHERE str = 's3'");
	assert (2 == stmt3.execute());

	Statement stmt4(tmp << "DELETE FROM Strings WHERE str = 's1'");
	assert (1 == stmt4.execute());

	Statement stmt5(tmp << "DELETE FROM Strings WHERE str = 'bad value'");
	assert (0 == stmt5.execute());

	Statement stmt6(tmp << "DELETE FROM Strings");
	assert (3 == stmt6.execute());
}


void SQLiteTest::testInsertSingleBulk()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::size_t x = 0;
	Statement stmt((tmp << "INSERT INTO Strings VALUES(:str)", use(x)));

	for (std::size_t i = 0; x < 100; ++x)
	{
		i = stmt.execute();
		assert (1 == i);
	}

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 100);
	tmp << "SELECT SUM(str) FROM Strings", into(count), now;
	assert (count == ((0+99)*100/2));
}


void SQLiteTest::testInsertSingleBulkVec()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::vector<int> data;
	data.push_back(0);
	data.push_back(1);

	Statement stmt((tmp << "INSERT INTO Strings VALUES(:str)", use(data)));
	
	for (int x = 0; x < 100; x += 2)
	{
		data[0] = x;
		data[1] = x+1;
		stmt.execute();
	}
	int count = 0;
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 100);
	tmp << "SELECT SUM(str) FROM Strings", into(count), now;
	assert (count == ((0+99)*100/2));
}


void SQLiteTest::testLimit()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::vector<int> data;
	for (int x = 0; x < 100; ++x)
	{
		data.push_back(x);
	}

	tmp << "INSERT INTO Strings VALUES(:str)", use(data), now;
	std::vector<int> retData;
	tmp << "SELECT * FROM Strings", into(retData), limit(50), now;
	assert (retData.size() == 50);
	for (int x = 0; x < 50; ++x)
	{
		assert(data[x] == retData[x]);
	}
}


void SQLiteTest::testLimitZero()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::vector<int> data;
	for (int x = 0; x < 100; ++x)
	{
		data.push_back(x);
	}

	tmp << "INSERT INTO Strings VALUES(:str)", use(data), now;
	std::vector<int> retData;
	tmp << "SELECT * FROM Strings", into(retData), limit(0), now; // stupid test, but at least we shouldn't crash
	assert (retData.size() == 0);
}


void SQLiteTest::testLimitOnce()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::vector<int> data;
	for (int x = 0; x < 101; ++x)
	{
		data.push_back(x);
	}

	tmp << "INSERT INTO Strings VALUES(:str)", use(data), now;
	std::vector<int> retData;
	Statement stmt = (tmp << "SELECT * FROM Strings", into(retData), limit(50), now);
	assert (!stmt.done());
	assert (retData.size() == 50);
	stmt.execute();
	assert (!stmt.done());
	assert (retData.size() == 100);
	stmt.execute();
	assert (stmt.done());
	assert (retData.size() == 101);

	for (int x = 0; x < 101; ++x)
	{
		assert(data[x] == retData[x]);
	}
}


void SQLiteTest::testLimitPrepare()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::vector<int> data;
	for (int x = 0; x < 100; ++x)
	{
		data.push_back(x);
	}

	Statement stmtIns = (tmp << "INSERT INTO Strings VALUES(:str)", use(data));
	assert (100 == stmtIns.execute());

	std::vector<int> retData;
	Statement stmt = (tmp << "SELECT * FROM Strings", into(retData), limit(50));
	assert (retData.size() == 0);
	assert (!stmt.done());
	std::size_t rows = stmt.execute();
	assert (50 == rows);
	assert (!stmt.done());
	assert (retData.size() == 50);
	rows = stmt.execute();
	assert (50 == rows);
	assert (stmt.done());
	assert (retData.size() == 100);
	rows = stmt.execute(); // will restart execution!
	assert (50 == rows);
	assert (!stmt.done());
	assert (retData.size() == 150);
	for (int x = 0; x < 150; ++x)
	{
		assert(data[x%100] == retData[x]);
	}
}



void SQLiteTest::testPrepare()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	std::vector<int> data;
	for (int x = 0; x < 100; x += 2)
	{
		data.push_back(x);
	}

	{
		Statement stmt((tmp << "INSERT INTO Strings VALUES(:str)", use(data)));
	}
	// stmt should not have been executed when destroyed
	int count = 100;
	tmp << "SELECT COUNT(*) FROM Strings", into(count), now;
	assert (count == 0);
}


void SQLiteTest::testSetSimple()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::set<std::string> lastNames;
	std::set<std::string> firstNames;
	std::set<std::string> addresses;
	std::set<int> ages;
	std::string tableName("Person");
	lastNames.insert("LN1");
	lastNames.insert("LN2");
	firstNames.insert("FN1");
	firstNames.insert("FN2");
	addresses.insert("ADDR1");
	addresses.insert("ADDR2");
	ages.insert(1);
	ages.insert(2);
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastNames), use(firstNames), use(addresses), use(ages), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::set<std::string> lastNamesR;
	std::set<std::string> firstNamesR;
	std::set<std::string> addressesR;
	std::set<int> agesR;
	tmp << "SELECT * FROM PERSON", into(lastNamesR), into(firstNamesR), into(addressesR), into(agesR), now;
	assert (ages == agesR);
	assert (lastNames == lastNamesR);
	assert (firstNames == firstNamesR);
	assert (addresses == addressesR);
}


void SQLiteTest::testSetComplex()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::set<Person> people;
	people.insert(Person("LN1", "FN1", "ADDR1", 1));
	people.insert(Person("LN2", "FN2", "ADDR2", 2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::set<Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result == people);
}


void SQLiteTest::testSetComplexUnique()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::vector<Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	people.push_back(p1);
	people.push_back(p1);
	people.push_back(p1);
	people.push_back(p1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.push_back(p2);

	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 5);

	std::set<Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result.size() == 2);
	assert (*result.begin() == p1);
	assert (*++result.begin() == p2);
}

void SQLiteTest::testMultiSetSimple()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multiset<std::string> lastNames;
	std::multiset<std::string> firstNames;
	std::multiset<std::string> addresses;
	std::multiset<int> ages;
	std::string tableName("Person");
	lastNames.insert("LN1");
	lastNames.insert("LN2");
	firstNames.insert("FN1");
	firstNames.insert("FN2");
	addresses.insert("ADDR1");
	addresses.insert("ADDR2");
	ages.insert(1);
	ages.insert(2);
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastNames), use(firstNames), use(addresses), use(ages), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::multiset<std::string> lastNamesR;
	std::multiset<std::string> firstNamesR;
	std::multiset<std::string> addressesR;
	std::multiset<int> agesR;
	tmp << "SELECT * FROM PERSON", into(lastNamesR), into(firstNamesR), into(addressesR), into(agesR), now;
	assert (ages.size() == agesR.size());
	assert (lastNames.size() == lastNamesR.size());
	assert (firstNames.size() == firstNamesR.size());
	assert (addresses.size() == addressesR.size());
}


void SQLiteTest::testMultiSetComplex()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multiset<Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	people.insert(p1);
	people.insert(p1);
	people.insert(p1);
	people.insert(p1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(p2);

	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 5);

	std::multiset<Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result.size() == people.size());
}


void SQLiteTest::testMapComplex()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::map<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN2", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);

	std::map<std::string, Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result == people);
}


void SQLiteTest::testMapComplexUnique()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN2", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 5);

	std::map<std::string, Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result.size() == 2);
}


void SQLiteTest::testMultiMapComplex()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN2", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 5);

	std::multimap<std::string, Person> result;
	tmp << "SELECT * FROM PERSON", into(result), now;
	assert (result.size() == people.size());
}


void SQLiteTest::testSelectIntoSingle()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	tmp << "SELECT * FROM PERSON", into(result), limit(1), now; // will return 1 object into one single result
	assert (result == p1);
}


void SQLiteTest::testSelectIntoSingleStep()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	Statement stmt = (tmp << "SELECT * FROM PERSON", into(result), limit(1)); 
	stmt.execute();
	assert (result == p1);
	assert (!stmt.done());
	stmt.execute();
	assert (result == p2);
	assert (stmt.done());
}


void SQLiteTest::testSelectIntoSingleFail()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), limit(2, true), now;
	assert (count == 2);
	Person result;
	try
	{
		tmp << "SELECT * FROM PERSON", into(result), limit(1, true), now; // will fail now
		fail("hardLimit is set: must fail");
	}
	catch(Poco::Data::LimitException&)
	{
	}
}


void SQLiteTest::testLowerLimitOk()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	try
	{
		tmp << "SELECT * FROM PERSON", into(result), lowerLimit(2), now; // will return 2 objects into one single result but only room for one!
		fail("Not enough space for results");
	}
	catch(Poco::Exception&)
	{
	}
}


void SQLiteTest::testSingleSelect()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	Statement stmt = (tmp << "SELECT * FROM PERSON", into(result), limit(1));
	stmt.execute();
	assert (result == p1);
	assert (!stmt.done());
	stmt.execute();
	assert (result == p2);
	assert (stmt.done());
}


void SQLiteTest::testLowerLimitFail()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	try
	{
		tmp << "SELECT * FROM PERSON", into(result), lowerLimit(3), now; // will fail
		fail("should fail. not enough data");
	}
	catch(Poco::Exception&)
	{
	}
}


void SQLiteTest::testCombinedLimits()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;

	std::string a, b, c;
	Statement stmt = (tmp << "SELECT LastName, FirstName, Address FROM Person WHERE Address = 'invalid value'",
		into(a), into(b), into(c), limit(1));
	assert (!stmt.done() && stmt.execute() == 0);
	
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	std::vector <Person> result;
	tmp << "SELECT * FROM PERSON", into(result), lowerLimit(2), upperLimit(2), now; // will return 2 objects
	assert (result.size() == 2);
	assert (result[0] == p1);
	assert (result[1] == p2);
}



void SQLiteTest::testRange()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	std::vector <Person> result;
	tmp << "SELECT * FROM PERSON", into(result), range(2, 2), now; // will return 2 objects
	assert (result.size() == 2);
	assert (result[0] == p1);
	assert (result[1] == p2);
}


void SQLiteTest::testCombinedIllegalLimits()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	try
	{
		tmp << "SELECT * FROM PERSON", into(result), lowerLimit(3), upperLimit(2), now;
		fail("lower > upper is not allowed");
	}
	catch(LimitException&)
	{
	}
}



void SQLiteTest::testIllegalRange()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(people), now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 2);
	Person result;
	try
	{
		tmp << "SELECT * FROM PERSON", into(result), range(3, 2), now;
		fail("lower > upper is not allowed");
	}
	catch(LimitException&)
	{
	}
}


void SQLiteTest::testEmptyDB()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	int count = 0;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 0);
	Person result;
	Statement stmt = (tmp << "SELECT * FROM PERSON", into(result), limit(1));
	stmt.execute();
	assert (result.getFirstName().empty());
	assert (stmt.done());
}


void SQLiteTest::testCLOB()
{
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Image BLOB)", now;
	CLOB img("0123456789", 10);
	int count = 0;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :img)", use(lastName), use(firstName), use(address), use(img), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	CLOB res;
	poco_assert (res.size() == 0);

	tmp << "SELECT Image FROM Person WHERE LastName == :ln", bind("lastname"), into(res), now;
	poco_assert (res == img);

	tmp << "DROP TABLE IF EXISTS BlobTest", now;
	std::vector<CLOB> resVec;
	const int arrSize = 10;
	char val[arrSize];
	for (int i = 0; i < arrSize; ++i)
	{
		val[i] = (char) (0x30 + i);
	}

	for (int i = 0; i < arrSize; ++i)
	{
		tmp << "CREATE TABLE IF NOT EXISTS BlobTest (idx INTEGER(2), Image BLOB)", now;
		val[0] = (char) (0x30 + i);
		img.assignRaw(val, arrSize);
		tmp << "INSERT INTO BlobTest VALUES(?, ?)", use(i), use(img), now;
	}
	tmp << "SELECT Image FROM BlobTest", into(resVec), now;
	poco_assert(resVec.size() == arrSize);
	for (int i = 0; i < arrSize; ++i)
	{
		poco_assert(*resVec[i].begin() == (char) (0x30 + i));
	}
}


void SQLiteTest::testTuple10()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER, int9 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int,int,int,int> t(0,1,2,3,4,5,6,7,8,9);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?,?,?)", use(t), now;

	Tuple<int,int,int,int,int,int,int,int,int,int> ret(-10,-11,-12,-13,-14,-15,-16,-17,-18,-19);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector10()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER, int9 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int,int,int,int> t(0,1,2,3,4,5,6,7,8,9);
	Tuple<int,int,int,int,int,int,int,int,int,int> t10(10,11,12,13,14,15,16,17,18,19);
	Tuple<int,int,int,int,int,int,int,int,int,int> t100(100,101,102,103,104,105,106,107,108,109);
	std::vector<Tuple<int,int,int,int,int,int,int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int,int,int,int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple9()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int,int,int> t(0,1,2,3,4,5,6,7,8);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?,?)", use(t), now;

	Tuple<int,int,int,int,int,int,int,int,int> ret(-10,-11,-12,-13,-14,-15,-16,-17,-18);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector9()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER, int8 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int,int,int> t(0,1,2,3,4,5,6,7,8);
	Tuple<int,int,int,int,int,int,int,int,int> t10(10,11,12,13,14,15,16,17,18);
	Tuple<int,int,int,int,int,int,int,int,int> t100(100,101,102,103,104,105,106,107,108);
	std::vector<Tuple<int,int,int,int,int,int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int,int,int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple8()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int,int> t(0,1,2,3,4,5,6,7);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?)", use(t), now;

	Tuple<int,int,int,int,int,int,int,int> ret(-10,-11,-12,-13,-14,-15,-16,-17);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector8()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER, "
		"int7 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int,int> t(0,1,2,3,4,5,6,7);
	Tuple<int,int,int,int,int,int,int,int> t10(10,11,12,13,14,15,16,17);
	Tuple<int,int,int,int,int,int,int,int> t100(100,101,102,103,104,105,106,107);
	std::vector<Tuple<int,int,int,int,int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int,int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple7()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int> t(0,1,2,3,4,5,6);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?)", use(t), now;

	Tuple<int,int,int,int,int,int,int> ret(-10,-11,-12,-13,-14,-15,-16);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector7()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER, int6 INTEGER)", now;

	Tuple<int,int,int,int,int,int,int> t(0,1,2,3,4,5,6);
	Tuple<int,int,int,int,int,int,int> t10(10,11,12,13,14,15,16);
	Tuple<int,int,int,int,int,int,int> t100(100,101,102,103,104,105,106);
	std::vector<Tuple<int,int,int,int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple6()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER)", now;

	Tuple<int,int,int,int,int,int> t(0,1,2,3,4,5);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?)", use(t), now;

	Tuple<int,int,int,int,int,int> ret(-10,-11,-12,-13,-14,-15);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector6()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER, int5 INTEGER)", now;

	Tuple<int,int,int,int,int,int> t(0,1,2,3,4,5);
	Tuple<int,int,int,int,int,int> t10(10,11,12,13,14,15);
	Tuple<int,int,int,int,int,int> t100(100,101,102,103,104,105);
	std::vector<Tuple<int,int,int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple5()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER)", now;

	Tuple<int,int,int,int,int> t(0,1,2,3,4);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?)", use(t), now;

	Tuple<int,int,int,int,int> ret(-10,-11,-12,-13,-14);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector5()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER, int4 INTEGER)", now;

	Tuple<int,int,int,int,int> t(0,1,2,3,4);
	Tuple<int,int,int,int,int> t10(10,11,12,13,14);
	Tuple<int,int,int,int,int> t100(100,101,102,103,104);
	std::vector<Tuple<int,int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple4()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER)", now;

	Tuple<int,int,int,int> t(0,1,2,3);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?)", use(t), now;

	Tuple<int,int,int,int> ret(-10,-11,-12,-13);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector4()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER, int3 INTEGER)", now;

	Tuple<int,int,int,int> t(0,1,2,3);
	Tuple<int,int,int,int> t10(10,11,12,13);
	Tuple<int,int,int,int> t100(100,101,102,103);
	std::vector<Tuple<int,int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple3()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER)", now;

	Tuple<int,int,int> t(0,1,2);

	tmp << "INSERT INTO Tuples VALUES (?,?,?)", use(t), now;

	Tuple<int,int,int> ret(-10,-11,-12);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector3()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples "
		"(int0 INTEGER, int1 INTEGER, int2 INTEGER)", now;

	Tuple<int,int,int> t(0,1,2);
	Tuple<int,int,int> t10(10,11,12);
	Tuple<int,int,int> t100(100,101,102);
	std::vector<Tuple<int,int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple2()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples (int0 INTEGER, int1 INTEGER)", now;

	Tuple<int,int> t(0,1);

	tmp << "INSERT INTO Tuples VALUES (?,?)", use(t), now;

	Tuple<int,int> ret(-10,-11);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector2()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples (int0 INTEGER, int1 INTEGER)", now;

	Tuple<int,int> t(0,1);
	Tuple<int,int> t10(10,11);
	Tuple<int,int> t100(100,101);
	std::vector<Tuple<int,int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?,?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int,int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testTuple1()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples (int0 INTEGER)", now;

	Tuple<int> t(0);

	tmp << "INSERT INTO Tuples VALUES (?)", use(t), now;

	Tuple<int> ret(-10);
	assert (ret != t);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == t);
}


void SQLiteTest::testTupleVector1()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Tuples", now;
	tmp << "CREATE TABLE Tuples (int0 INTEGER)", now;

	Tuple<int> t(0);
	Tuple<int> t10(10);
	Tuple<int> t100(100);
	std::vector<Tuple<int> > v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	tmp << "INSERT INTO Tuples VALUES (?)", use(v), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Tuples", into(count), now;
	assert (v.size() == count);

	std::vector<Tuple<int> > ret;
	assert (ret != v);
	tmp << "SELECT * FROM Tuples", into(ret), now;
	assert (ret == v);
}


void SQLiteTest::testDateTime()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS DateTimes", now;
	tmp << "CREATE TABLE DateTimes (dt0 DATE)", now;

	DateTime dt(1965, 6, 18, 5, 35, 1);
	tmp << "INSERT INTO DateTimes VALUES (?)", use(dt), now;

	DateTime rdt;
	assert (rdt != dt);
	tmp << "SELECT * FROM DateTimes", into(rdt), now;
	assert (rdt == dt);

	tmp << "DELETE FROM DateTimes", now;

	Date d(dt);
	tmp << "INSERT INTO DateTimes VALUES (?)", use(d), now;

	Date rd;
	assert (rd != d);
	tmp << "SELECT * FROM DateTimes", into(rd), now;
	assert (rd == d);

	tmp << "DELETE FROM DateTimes", now;

	Time t(dt);
	tmp << "INSERT INTO DateTimes VALUES (?)", use(t), now;

	Time rt;
	assert (rt != t);
	tmp << "SELECT * FROM DateTimes", into(rt), now;
	assert (rt == t);
}


void SQLiteTest::testInternalExtraction()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Vectors", now;
	tmp << "CREATE TABLE Vectors (int0 INTEGER, flt0 REAL, str0 VARCHAR)", now;

	std::vector<Tuple<int, double, std::string> > v;
	v.push_back(Tuple<int, double, std::string>(1, 1.5, "3"));
	v.push_back(Tuple<int, double, std::string>(2, 2.5, "4"));
	v.push_back(Tuple<int, double, std::string>(3, 3.5, "5"));
	v.push_back(Tuple<int, double, std::string>(4, 4.5, "6"));

	tmp << "INSERT INTO Vectors VALUES (?,?,?)", use(v), now;

	Statement stmt = (tmp << "SELECT * FROM Vectors", now);
	RecordSet rset(stmt);
	assert (3 == rset.columnCount());
	assert (4 == rset.rowCount());

	RecordSet rset2(rset);
	assert (3 == rset2.columnCount());
	assert (4 == rset2.rowCount());

	Int32 a = rset.value<Int32>(0,2);
	assert (3 == a);

	int c = rset2.value(0);
	assert (1 == c);

	Int32 b = rset2.value<Int32>("InT0",2);
	assert (3 == b);

	double d = rset.value<double>(1,0);
	assert (1.5 == d);

	std::string s = rset.value<std::string>(2,1);
	assert ("4" == s);
	
	typedef std::deque<Int32> IntDeq;
	
	const Column<IntDeq>& col = rset.column<IntDeq>(0);
	assert (col[0] == 1);

	try { rset.column<IntDeq>(100); fail ("must fail"); }
	catch (RangeException&) { }

	const Column<IntDeq>& col1 = rset.column<IntDeq>(0);
	assert ("int0" == col1.name());
	Column<IntDeq>::Iterator it = col1.begin();
	Column<IntDeq>::Iterator itEnd = col1.end();
	int counter = 1;
	for (; it != itEnd; ++it, ++counter)
		assert (counter == *it);

	rset = (tmp << "SELECT COUNT(*) FROM Vectors", now);
	s = rset.value<std::string>(0,0);
	assert ("4" == s);
	
	stmt = (tmp << "DELETE FROM Vectors", now);
	rset = stmt;

	try { rset.column<IntDeq>(0); fail ("must fail"); }
	catch (RangeException&) { }
}


void SQLiteTest::testPrimaryKeyConstraint()
{
	Session ses (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	ses << "DROP TABLE IF EXISTS LogTest", now;
	ses << "CREATE TABLE LogTest (Id INTEGER PRIMARY KEY, Time INTEGER, Value INTEGER)", now;
	const double value = -200000000000.0;
	const Poco::Int64 timeIn = static_cast<Poco::Int64>(22329988776655.0);
	int id = 1;

	ses.begin();

	for(int i = 0; i < 10; i++)
	{
		try
		{
			ses << "INSERT INTO LogTest (Id, [Time], Value) VALUES (:id, :time, :value)", use(id), bind(timeIn), bind(value), now; //lint !e1058
			if (i > 0)
				fail("must fail");
		}
		catch(Poco::Exception&)
		{
			if (i == 0) // the very first insert must work
				throw;
		}
	}

	ses.commit(); 
}


void SQLiteTest::testNullable()
{
	Session ses (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	ses << "DROP TABLE IF EXISTS NullableTest", now;

	ses << "CREATE TABLE NullableTest (i INTEGER, r REAL, s VARCHAR, d DATETIME)", now;

	ses << "INSERT INTO NullableTest VALUES(:i, :r, :s, :d)", use(null), use(null), use(null), use(null), now;
	
	Nullable<int> i = 1;
	Nullable<double> f = 1.5;
	Nullable<std::string> s = std::string("abc");
	Nullable<DateTime> d = DateTime();

	assert (!i.isNull());
	assert (!f.isNull());
	assert (!s.isNull());
	assert (!d.isNull());

	ses << "SELECT i, r, s, d FROM NullableTest", into(i), into(f), into(s), into(d), now;

	assert (i.isNull());
	assert (f.isNull());
	assert (s.isNull());
	assert (d.isNull());

	RecordSet rs(ses, "SELECT * FROM NullableTest");

	rs.moveFirst();
	assert (rs.isNull("i"));
	assert (rs.isNull("r"));
	assert (rs.isNull("s"));
	assert (rs.isNull("d"));

	Var di = 1;
	Var df = 1.5;
	Var ds = "abc";
	Var dd = DateTime();

	assert (!di.isEmpty());
	assert (!df.isEmpty());
	assert (!ds.isEmpty());
	assert (!dd.isEmpty());

	ses << "SELECT i, r, s, d FROM NullableTest", into(di), into(df), into(ds), into(dd), now;

	assert (di.isEmpty());
	assert (df.isEmpty());
	assert (ds.isEmpty());
	assert (dd.isEmpty());
}


void SQLiteTest::testNulls()
{
	Session ses (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	ses << "DROP TABLE IF EXISTS NullTest", now;

	ses << "CREATE TABLE NullTest (i INTEGER NOT NULL)", now;

	try
	{
		ses << "INSERT INTO NullTest VALUES(:i)", use(null), now;
		fail ("must fail");
	}catch (ConstraintViolationException&) { }

	ses << "DROP TABLE IF EXISTS NullTest", now;
	ses << "CREATE TABLE NullTest (i INTEGER, r REAL, v VARCHAR)", now;

	ses << "INSERT INTO NullTest VALUES(:i, :r, :v)", use(null), use(null), use(null), now;
	
	RecordSet rs(ses, "SELECT i, r, v, null as e FROM NullTest");
	rs.moveFirst();
	assert (rs.isNull("i"));
	assert (rs["i"].isEmpty());
	assert (rs.isNull("r"));
	assert (rs.isNull("v"));
	assert (rs["v"].isEmpty());
	assert (rs["e"].isEmpty());

	assert (rs[0].isEmpty());
	assert (rs[1].isEmpty());
	assert (rs[2].isEmpty());
	assert (rs[3].isEmpty());

	ses << "DROP TABLE IF EXISTS NullTest", now;
	ses << "CREATE TABLE NullTest (i INTEGER, r REAL, v VARCHAR)", now;
	int i = 1;
	double f = 1.2;
	std::string s = "123";

	ses << "INSERT INTO NullTest (i, r, v) VALUES (:i, :r, :v)", use(i), use(f), use(s), now;
	rs = (ses << "SELECT * FROM NullTest", now);
	rs.moveFirst();
	assert (!rs.isNull("i"));
	assert (rs["i"] == 1);
	assert (!rs.isNull("v"));
	assert (!rs.isNull("r"));
	assert (rs["v"] == "123");
	
	ses << "UPDATE NullTest SET v = :n WHERE i == :i", use(null), use(i), now;
	i = 2;
	f = 3.4;
	ses << "INSERT INTO NullTest (i, r, v) VALUES (:i, :r, :v)", use(i), use(null), use(null), now;
	rs = (ses << "SELECT i, r, v FROM NullTest ORDER BY i ASC", now);
	rs.moveFirst();
	assert (!rs.isNull("i"));
	assert (rs["i"] == 1);
	assert (!rs.isNull("r"));
	assert (rs.isNull("v"));
	assert (rs["v"].isEmpty());

	assert (rs.moveNext());
	assert (!rs.isNull("i"));
	assert (rs["i"] == 2);
	Poco::Int64 i64 = 0;
	assert (rs.nvl("i", i64) == 2);
	assert (rs.nvl("i", 123) == 2);

	assert (rs.isNull("r"));
	assert (rs.nvl("r", 123) == 123);
	assert (rs.nvl("r", 1.5) == 1.5);

	assert (rs.isNull("v"));
	assert (rs["v"].isEmpty());
	assert (rs.nvl("v", s) == "123");
}


void SQLiteTest::testRowIterator()
{
	Session ses (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	ses << "DROP TABLE IF EXISTS Vectors", now;
	ses << "CREATE TABLE Vectors (int0 INTEGER, flt0 REAL, str0 VARCHAR)", now;

	std::vector<Tuple<int, double, std::string> > v;
	v.push_back(Tuple<int, double, std::string>(1, 1.5f, "3"));
	v.push_back(Tuple<int, double, std::string>(2, 2.5f, "4"));
	v.push_back(Tuple<int, double, std::string>(3, 3.5f, "5"));
	v.push_back(Tuple<int, double, std::string>(4, 4.5f, "6"));

	ses << "INSERT INTO Vectors VALUES (?,?,?)", use(v), now;

	RecordSet rset(ses, "SELECT * FROM Vectors");

	std::ostringstream osLoop;
	RecordSet::ConstIterator it = rset.begin();
	RecordSet::ConstIterator end = rset.end();
	for (int i = 1; it != end; ++it, ++i) 
	{
		assert (it->get(0) == i);
		osLoop << *it;
	}
	assert (!osLoop.str().empty());

	std::ostringstream osCopy;
	std::copy(rset.begin(), rset.end(), std::ostream_iterator<Row>(osCopy));
	assert (osLoop.str() == osCopy.str());
}


void SQLiteTest::testAsync()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Strings", now;
	tmp << "CREATE TABLE IF NOT EXISTS Strings (str INTEGER(10))", now;
	
	int rowCount = 500;
	std::vector<int> data(rowCount);
	Statement stmt = (tmp << "INSERT INTO Strings VALUES(:str)", use(data));
	Statement::Result result = stmt.executeAsync();
	assert (!stmt.isAsync());
	result.wait();
	assert (500 == result.data());

	Statement stmt1 = (tmp << "SELECT * FROM Strings", into(data), async, now);
	assert (stmt1.isAsync());
	assert (stmt1.wait() == rowCount);

	stmt1.execute();
	try {
		stmt1.execute();
		fail ("must fail");
	} catch (InvalidAccessException&)
	{
		stmt1.wait();
		stmt1.execute();
		stmt1.wait();
	}

	stmt = tmp << "SELECT * FROM Strings", into(data), async, now;
	assert (stmt.isAsync());
	stmt.wait();

	assert (stmt.execute() == 0);
	assert (stmt.isAsync());
	try {
		result = stmt.executeAsync();
		fail ("must fail");
	} catch (InvalidAccessException&)
	{
		stmt.wait();
		result = stmt.executeAsync();
	}

	assert (stmt.wait() == rowCount);
	assert (result.data() == rowCount);
	stmt.setAsync(false);
	assert (!stmt.isAsync());
	assert (stmt.execute() == rowCount);

	stmt = tmp << "SELECT * FROM Strings", into(data), sync, now;
	assert (!stmt.isAsync());
	assert (stmt.wait() == 0);
	assert (stmt.execute() == rowCount);
	result = stmt.executeAsync();
	assert (!stmt.isAsync());
	result.wait();
	assert (result.data() == rowCount);

	assert (0 == rowCount % 10);
	int step = (int) (rowCount/10);
	data.clear();
	Statement stmt2 = (tmp << "SELECT * FROM Strings", into(data), async, limit(step));
	assert (data.size() == 0);
	assert (!stmt2.done());
	std::size_t rows = 0;
	
	for (int i = 0; !stmt2.done(); i += step)
	{
		stmt2.execute();
		rows = stmt2.wait();
		assert (step == rows);
		assert (step + i == data.size());
	}
	assert (stmt2.done());
	assert (rowCount == data.size());

	stmt2 = tmp << "SELECT * FROM Strings", reset;
	assert (!stmt2.isAsync());
	assert ("deque" == stmt2.getStorage());
	assert (stmt2.execute() == rowCount);
}


void SQLiteTest::testAny()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Anys", now;
	tmp << "CREATE TABLE Anys (int0 INTEGER, flt0 REAL, str0 VARCHAR)", now;

	Any i = Int32(42);
	Any f = double(42.5);
	Any s = std::string("42");

	tmp << "INSERT INTO Anys VALUES (?, ?, ?)", use(i), use(f), use(s), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Anys", into(count), now;
	assert (1 == count);

	i = 0;
	f = 0.0;
	s = std::string("");
	tmp << "SELECT * FROM Anys", into(i), into(f), into(s), now;
	assert (AnyCast<Int32>(i) == 42);
	assert (AnyCast<double>(f) == 42.5);
	assert (AnyCast<std::string>(s) == "42");
}


void SQLiteTest::testDynamicAny()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS Anys", now;
	tmp << "CREATE TABLE Anys (int0 INTEGER, flt0 REAL, str0 VARCHAR, empty INTEGER)", now;

	DynamicAny i = Int32(42);
	DynamicAny f = double(42.5);
	DynamicAny s = std::string("42");
	DynamicAny e;
	assert (e.isEmpty());

	tmp << "INSERT INTO Anys VALUES (?, ?, ?, null)", use(i), use(f), use(s), now;

	int count = 0;
	tmp << "SELECT COUNT(*) FROM Anys", into(count), now;
	assert (1 == count);

	i = 0;
	f = 0.0;
	s = std::string("");
	e = 1;
	assert (!e.isEmpty());
	tmp << "SELECT * FROM Anys", into(i), into(f), into(s), into(e), now;
	assert (42 == i);
	assert (42.5 == f);
	assert ("42" == s);
	assert (e.isEmpty());
}


void SQLiteTest::testPair()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	std::string tableName("Simpsons");
	std::pair<std::string, int> junior = std::make_pair("Junior", 12);
	std::pair<std::string, int> senior = std::make_pair("Senior", 99);
	

	int count = 0;
	std::string result;

	tmp << "DROP TABLE IF EXISTS Simpsons", now;
	tmp << "CREATE TABLE IF NOT EXISTS Simpsons (LastName VARCHAR(30), Age INTEGER(3))", now;
	tmp << "SELECT name FROM sqlite_master WHERE tbl_name=?", use(tableName), into(result), now;
	assert (result == tableName);


	// these are fine
	tmp << "INSERT INTO Simpsons VALUES(?, ?)", use(junior), now;
	tmp << "INSERT INTO Simpsons VALUES(?, ?)", useRef(senior), now;

	tmp << "SELECT COUNT(*) FROM Simpsons", into(count), now;
	assert (2 == count);
	
	std::vector<std::pair<std::string, int> > ret;
	tmp << "SELECT * FROM Simpsons", into(ret), range(2,2), now;
	assert (ret[0].second == 12 || ret[1].second == 12);
	assert (ret[0].second == 99 || ret[1].second == 99);
	assert (ret[0].first == "Junior" || ret[1].first == "Junior");
	assert (ret[0].first == "Senior" || ret[1].first == "Senior");
	
}


void SQLiteTest::testSQLChannel()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS T_POCO_LOG", now;
	tmp << "CREATE TABLE T_POCO_LOG (Source VARCHAR,"
		"Name VARCHAR,"
		"ProcessId INTEGER,"
		"Thread VARCHAR, "
		"ThreadId INTEGER," 
		"Priority INTEGER,"
		"Text VARCHAR,"
		"DateTime DATE)", now;

	tmp << "DROP TABLE IF EXISTS T_POCO_LOG_ARCHIVE", now;
	tmp << "CREATE TABLE T_POCO_LOG_ARCHIVE (Source VARCHAR,"
		"Name VARCHAR,"
		"ProcessId INTEGER,"
		"Thread VARCHAR, "
		"ThreadId INTEGER," 
		"Priority INTEGER,"
		"Text VARCHAR,"
		"DateTime DATE)", now;

	AutoPtr<SQLChannel> pChannel = new SQLChannel(Poco::Data::SQLite::Connector::KEY, "dummy.db", "TestSQLChannel");
	pChannel->setProperty("keep", "2 seconds");

	Message msgInf("InformationSource", "a Informational async message", Message::PRIO_INFORMATION);
	pChannel->log(msgInf);
	Message msgWarn("WarningSource", "b Warning async message", Message::PRIO_WARNING);
	pChannel->log(msgWarn);
	pChannel->wait();

	pChannel->setProperty("async", "false");
	Message msgInfS("InformationSource", "c Informational sync message", Message::PRIO_INFORMATION);
	pChannel->log(msgInfS);
	Message msgWarnS("WarningSource", "d Warning sync message", Message::PRIO_WARNING);
	pChannel->log(msgWarnS);

	RecordSet rs(tmp, "SELECT * FROM T_POCO_LOG ORDER by Text");
	assert (4 == rs.rowCount());
	assert ("InformationSource" == rs["Source"]);
	assert ("a Informational async message" == rs["Text"]);
	rs.moveNext();
	assert ("WarningSource" == rs["Source"]);
	assert ("b Warning async message" == rs["Text"]);
	rs.moveNext();
	assert ("InformationSource" == rs["Source"]);
	assert ("c Informational sync message" == rs["Text"]);
	rs.moveNext();
	assert ("WarningSource" == rs["Source"]);
	assert ("d Warning sync message" == rs["Text"]);

	Thread::sleep(3000);

	Message msgInfA("InformationSource", "e Informational sync message", Message::PRIO_INFORMATION);
	pChannel->log(msgInfA);
	Message msgWarnA("WarningSource", "f Warning sync message", Message::PRIO_WARNING);
	pChannel->log(msgWarnA);

	RecordSet rs1(tmp, "SELECT * FROM T_POCO_LOG_ARCHIVE");
	assert (4 == rs1.rowCount());

	pChannel->setProperty("keep", "");
	assert ("forever" == pChannel->getProperty("keep"));
	RecordSet rs2(tmp, "SELECT * FROM T_POCO_LOG ORDER by Text");
	assert (2 == rs2.rowCount());
	assert ("InformationSource" == rs2["Source"]);
	assert ("e Informational sync message" == rs2["Text"]);
	rs2.moveNext();
	assert ("WarningSource" == rs2["Source"]);
	assert ("f Warning sync message" == rs2["Text"]);
}


void SQLiteTest::testSQLLogger()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	tmp << "DROP TABLE IF EXISTS T_POCO_LOG", now;
	tmp << "CREATE TABLE T_POCO_LOG (Source VARCHAR,"
		"Name VARCHAR,"
		"ProcessId INTEGER,"
		"Thread VARCHAR, "
		"ThreadId INTEGER," 
		"Priority INTEGER,"
		"Text VARCHAR,"
		"DateTime DATE)", now;

	{
		AutoPtr<SQLChannel> pChannel = new SQLChannel(Poco::Data::SQLite::Connector::KEY, "dummy.db", "TestSQLChannel");
		Logger& root = Logger::root();
		root.setChannel(pChannel.get());
		root.setLevel(Message::PRIO_INFORMATION);
		
		root.information("Informational message");
		root.warning("Warning message");
		root.debug("Debug message");
	}

	Thread::sleep(100);
	RecordSet rs(tmp, "SELECT * FROM T_POCO_LOG ORDER by DateTime");
	assert (2 == rs.rowCount());
	assert ("TestSQLChannel" == rs["Source"]);
	assert ("Informational message" == rs["Text"]);
	rs.moveNext();
	assert ("TestSQLChannel" == rs["Source"]);
	assert ("Warning message" == rs["Text"]);
}


void SQLiteTest::testExternalBindingAndExtraction()
{
	AbstractExtractionVecVec extractionVec;
	AbstractExtractionVec extraction;
	AbstractBindingVec binding;

	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");

	tmp << "DROP TABLE IF EXISTS Ints", now;
	tmp << "CREATE TABLE Ints (int0 INTEGER, int1 INTEGER, int2 INTEGER)", now;

	int x = 1, y = 2, z = 3;
	binding.push_back(use(x));
	binding.push_back(use(y));
	binding.push_back(use(z));

	tmp << "INSERT INTO Ints VALUES (?,?,?)", use(binding), now;

	Poco::Int64 a = 0, b = 0, c = 0;
	extraction.push_back(into(a));
	extraction.push_back(into(b));
	extraction.push_back(into(c));
	tmp << "SELECT * FROM Ints", into(extraction), now;
	assert (a == x);
	assert (b == y);
	assert (c == z);

	a = 0, b = 0, c = 0;
	extractionVec.push_back(extraction);
	tmp << "SELECT * FROM Ints", into(extractionVec), now;
	assert (a == x);
	assert (b == y);
	assert (c == z);
}


void SQLiteTest::testBindingCount()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");

	tmp << "DROP TABLE IF EXISTS Ints", now;
	tmp << "CREATE TABLE Ints (int0 INTEGER)", now;

	int i = 42;
	try	{ tmp << "INSERT INTO Ints VALUES (?)", now; } 
	catch (ParameterCountMismatchException&) { }
	tmp << "INSERT INTO Ints VALUES (?)", use(i), now;

	i = 0;
	try	{ tmp << "SELECT int0 from Ints where int0 = ?", into(i), now; }
	catch (ParameterCountMismatchException&) { }
	tmp << "SELECT int0 from Ints where int0 = ?", bind(42), into(i), now;
	assert (42 == i);

	tmp << "DROP TABLE IF EXISTS Ints", now;
	tmp << "CREATE TABLE Ints (int0 INTEGER, int1 INTEGER, int2 INTEGER)", now;

	try	{ tmp << "INSERT INTO Ints VALUES (?,?,?)", bind(42), bind(42), now; }
	catch (ParameterCountMismatchException&) { }
}


void SQLiteTest::testMultipleResults()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");

	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE Person (LastName VARCHAR(30),"
		"FirstName VARCHAR(30),"
		"Address VARCHAR(30),"
		"Age INTEGER)", now;

	typedef Tuple<std::string, std::string, std::string, Poco::UInt32> Person;
	std::vector<Person> people, people2;
	people.push_back(Person("Simpson", "Homer", "Springfield", 42));
	people.push_back(Person("Simpson", "Bart", "Springfield", 12));
	people.push_back(Person("Simpson", "Lisa", "Springfield", 10));

	Person pHomer;
	int aHomer = 42, aLisa = 10;
	Poco::UInt32 aBart = 0;

	Poco::UInt32 pos1 = 1;
	int pos2 = 2;

	Statement stmt(tmp);
	stmt << "INSERT INTO Person VALUES (?, ?, ?, ?);"
		"SELECT * FROM Person WHERE Age = ?; "
		"SELECT Age FROM Person WHERE FirstName = 'Bart'; "
		"SELECT * FROM Person WHERE Age = ? OR Age = ? ORDER BY Age;"
		, use(people)
		, into(pHomer, from(0)), use(aHomer)
		, into(aBart, pos1)
		, into(people2, from(pos2)), use(aLisa), use(aHomer);

	assert (7 == stmt.execute());
	assert (Person("Simpson", "Homer", "Springfield", 42) == pHomer);
	assert (12 == aBart);
	assert (2 == people2.size());
	assert (Person("Simpson", "Lisa", "Springfield", 10) == people2[0]);
	assert (Person("Simpson", "Homer", "Springfield", 42) == people2[1]);
}


void SQLiteTest::testReconnect()
{
	Session session (Poco::Data::SQLite::Connector::KEY, "dummy.db");

	session << "DROP TABLE IF EXISTS Person", now;
	session << "CREATE TABLE Person (LastName VARCHAR(30),"
		"FirstName VARCHAR(30),"
		"Address VARCHAR(30),"
		"Age INTEGER)", now;

	std::string lastName = "lastName";
	std::string firstName("firstName");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;

	session << "INSERT INTO PERSON VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(age), now;

	count = 0;
	session << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);

	assert (session.isConnected());
	session.close();
	assert (!session.isConnected());
	try 
	{
		session << "SELECT LastName FROM PERSON", into(result), now;  
		fail ("must fail");
	}
	catch(NotConnectedException&){ }
	assert (!session.isConnected());

	session.open();
	assert (session.isConnected());
	session << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
	assert (session.isConnected());
}


void SQLiteTest::testThreadModes()
{
	using namespace Poco::Data::SQLite;
	typedef std::vector<int> ModeVec;

	assert (Utility::isThreadSafe());
	assert (Utility::getThreadMode() == Utility::THREAD_MODE_SERIAL);

	const int datasize = 100;
	ModeVec mode;
	mode.push_back(Utility::THREAD_MODE_SINGLE);
	mode.push_back(Utility::THREAD_MODE_MULTI);
	mode.push_back(Utility::THREAD_MODE_SERIAL);

	Poco::Stopwatch sw;
	ModeVec::iterator it = mode.begin();
	ModeVec::iterator end = mode.end();
	for (; it != end; ++it)
	{
		sw.restart();
		assert (Utility::setThreadMode(*it));
		{
			Session tmp (Connector::KEY, "dummy.db");
			std::vector<int> iv(datasize);
			int count = 0;

			tmp << "DROP TABLE IF EXISTS Ints", now;
			tmp << "CREATE TABLE IF NOT EXISTS Ints (theInt INTEGER)", now;
			Statement stmt((tmp << "INSERT INTO Ints VALUES(?)", use(iv)));
			tmp << "SELECT COUNT(*) FROM Ints", into(count), now;
			assert (count == 0);
			stmt.execute();
			tmp << "SELECT COUNT(*) FROM Ints", into(count), now;
			assert (count == datasize);
			count = 0;
			tmp << "SELECT COUNT(*) FROM Ints", into(count), now;
			assert (count == datasize);
		}
		sw.stop();
		std::cout << "Mode: " << ((*it == Utility::THREAD_MODE_SINGLE) ? "single,"
                                :(*it == Utility::THREAD_MODE_MULTI) ? "multi,"
                                :(*it == Utility::THREAD_MODE_SERIAL) ? "serial,"
                                : "unknown,") << " Time: " << sw.elapsed() / 1000.0 << " [ms]" << std::endl;
	}

	assert (Utility::setThreadMode(Utility::THREAD_MODE_SERIAL));
	assert (Utility::isThreadSafe());
	assert (Utility::getThreadMode() == Utility::THREAD_MODE_SERIAL);
}


void SQLiteTest::sqliteUpdateCallbackFn(void* pVal, int opCode, const char* pDB, const char* pTable, Poco::Int64 row)
{
	poco_check_ptr(pVal);
	Poco::Int64* pV = reinterpret_cast<Poco::Int64*>(pVal);
	if (opCode == Utility::OPERATION_INSERT)
	{
		poco_assert (*pV == 2);
		poco_assert (row == 1);
		std::cout << "Inserted " << pDB << '.' << pTable << ", RowID=" << row << std::endl;
		++_insertCounter;
	}
	else if (opCode == Utility::OPERATION_UPDATE)
	{
		poco_assert (*pV == 3);
		poco_assert (row == 1);
		std::cout << "Updated " << pDB << '.' << pTable << ", RowID=" << row << std::endl;
		++_updateCounter;
	}
	else if (opCode == Utility::OPERATION_DELETE)
	{
		poco_assert (*pV == 4);
		poco_assert (row == 1);
		std::cout << "Deleted " << pDB << '.' << pTable << ", RowID=" << row << std::endl;
		++_deleteCounter;
	}
}


void SQLiteTest::testUpdateCallback()
{
	// will be updated by callback
	_insertCounter = 0;
	_updateCounter = 0;
	_deleteCounter = 0;

	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	Poco::Int64 val = 1;
	assert (Utility::registerUpdateHandler(tmp, &sqliteUpdateCallbackFn, &val));

	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "SELECT name FROM sqlite_master WHERE tbl_name=?", use(tableName), into(result), now;
	assert (result == tableName);

	// insert
	val = 2;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	assert (_insertCounter == 1);
	tmp << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	tmp << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
	
	// update
	val = 3;
	tmp << "UPDATE PERSON SET Age = -1", now;
	tmp << "SELECT Age FROM PERSON", into(age), now;
	assert (-1 == age);
	assert (_updateCounter == 1);
	
	// delete
	val =4;
	tmp << "DELETE FROM Person WHERE Age = -1", now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 0);
	assert (_deleteCounter == 1);

	// disarm callback and do the same drill
	assert (Utility::registerUpdateHandler(tmp, (Utility::UpdateCallbackType) 0, &val));
	
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "SELECT name FROM sqlite_master WHERE tbl_name=?", use(tableName), into(result), now;
	assert (result == tableName);

	// must remain zero now
	_insertCounter = 0;
	_updateCounter = 0;
	_deleteCounter = 0;

	// insert
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 1);
	assert (_insertCounter == 0);
	tmp << "SELECT LastName FROM PERSON", into(result), now;
	assert (lastName == result);
	tmp << "SELECT Age FROM PERSON", into(count), now;
	assert (count == age);
	
	// update
	tmp << "UPDATE PERSON SET Age = -1", now;
	tmp << "SELECT Age FROM PERSON", into(age), now;
	assert (-1 == age);
	assert (_updateCounter == 0);
	
	// delete
	tmp << "DELETE FROM Person WHERE Age = -1", now;
	tmp << "SELECT COUNT(*) FROM PERSON", into(count), now;
	assert (count == 0);
	assert (_deleteCounter == 0);

	tmp.close();
	assert (!tmp.isConnected());
}


int SQLiteTest::sqliteCommitCallbackFn(void* pVal)
{
	poco_check_ptr(pVal);
	Poco::Int64* pV = reinterpret_cast<Poco::Int64*>(pVal);
	poco_assert ((*pV) == 1);
	++(*pV);
	return 0;
}


void SQLiteTest::testCommitCallback()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	Poco::Int64 val = 1;
	assert (Utility::registerUpdateHandler(tmp, &sqliteCommitCallbackFn, &val));

	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	std::string result;
	tmp.begin();
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp.commit();
	assert (val == 2);

	assert (Utility::registerUpdateHandler(tmp, (Utility::CommitCallbackType) 0, &val));
	val = 0;
	tmp.begin();
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp.commit();
	assert (val == 0);

}


void SQLiteTest::sqliteRollbackCallbackFn(void* pVal)
{
	poco_check_ptr(pVal);
	Poco::Int64* pV = reinterpret_cast<Poco::Int64*>(pVal);
	poco_assert ((*pV) == 1);
	++(*pV);
}


void SQLiteTest::testRollbackCallback()
{
	Session tmp (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (tmp.isConnected());
	Poco::Int64 val = 1;
	assert (Utility::registerUpdateHandler(tmp, &sqliteRollbackCallbackFn, &val));

	std::string tableName("Person");
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	int age = 133132;
	std::string result;
	tmp.begin();
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp.rollback();
	assert (val == 2);

	assert (Utility::registerUpdateHandler(tmp, (Utility::RollbackCallbackType) 0, &val));
	val = 0;
	tmp.begin();
	tmp << "DROP TABLE IF EXISTS Person", now;
	tmp << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;
	tmp << "INSERT INTO PERSON VALUES(:ln, :fn, :ad, :age)", use(lastName), use(firstName), use(address), use(age), now;
	tmp.rollback();
	assert (val == 0);
}


void SQLiteTest::testNotifier()
{
	Session session (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (session.isConnected());
	session << "DROP TABLE IF EXISTS Person", now;
	session << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	Notifier notifier(session);
	notifier.insert += delegate(this, &SQLiteTest::onInsert);
	notifier.update += delegate(this, &SQLiteTest::onUpdate);
	notifier.erase += delegate(this, &SQLiteTest::onDelete);

	_insertCounter = 0;
	_updateCounter = 0;
	_deleteCounter = 0;

	session << "INSERT INTO PERSON VALUES('Simpson', 'Bart', 'Springfield', 12)", now;
	assert (_insertCounter == 1);
	assert (notifier.getRow() == 1);
	session << "INSERT INTO PERSON VALUES('Simpson', 'Lisa', 'Springfield', 10)", now;
	assert (_insertCounter == 2);
	assert (notifier.getRow() == 2);
	session << "INSERT INTO PERSON VALUES('Simpson', 'Homer', 'Springfield', 42)", now;
	assert (_insertCounter == 3);
	assert (notifier.getRow() == 3);

	session << "UPDATE PERSON SET Age = 11 WHERE FirstName = 'Bart'", now;
	assert (_updateCounter == 1);
	assert (notifier.getRow() == 1);
	session << "UPDATE PERSON SET Age = 9 WHERE FirstName = 'Lisa'", now;
	assert (_updateCounter == 2);
	assert (notifier.getRow() == 2);
	session << "UPDATE PERSON SET Age = 41 WHERE FirstName = 'Homer'", now;
	assert (_updateCounter == 3);
	assert (notifier.getRow() == 3);

	notifier.setRow(0);
	// SQLite optimizes DELETE so here we must have
	// the WHERE clause to trigger per-row notifications
	session << "DELETE FROM PERSON WHERE 1=1", now;
	assert (_deleteCounter == 3);
	assert (notifier.getRow() == 3);

	notifier.insert -= delegate(this, &SQLiteTest::onInsert);
	notifier.update -= delegate(this, &SQLiteTest::onUpdate);
	notifier.erase -= delegate(this, &SQLiteTest::onDelete);

	notifier.disableUpdate();

	notifier.setRow(0);
	_commitCounter = 0;
	notifier.commit += delegate(this, &SQLiteTest::onCommit);
	session.begin();
	session << "INSERT INTO PERSON VALUES('Simpson', 'Bart', 'Springfield', 12)", now;
	session << "INSERT INTO PERSON VALUES('Simpson', 'Lisa', 'Springfield', 10)", now;
	session << "INSERT INTO PERSON VALUES('Simpson', 'Homer', 'Springfield', 42)", now;
	session.commit();
	assert (_commitCounter == 1);
	assert (notifier.getRow() == 0);
	notifier.commit -= delegate(this, &SQLiteTest::onCommit);

	session << "DELETE FROM PERSON", now;

	notifier.setRow(0);
	_rollbackCounter = 0;
	notifier.rollback += delegate(this, &SQLiteTest::onRollback);
	session.begin();
	session << "INSERT INTO PERSON VALUES('Simpson', 'Bart', 'Springfield', 12)", now;
	session << "INSERT INTO PERSON VALUES('Simpson', 'Lisa', 'Springfield', 10)", now;
	session << "INSERT INTO PERSON VALUES('Simpson', 'Homer', 'Springfield', 42)", now;
	session.rollback();
	assert (_rollbackCounter == 1);
	assert (notifier.getRow() == 0);
	notifier.rollback -= delegate(this, &SQLiteTest::onRollback);
}


void SQLiteTest::onInsert(const void* pSender)
{
	Notifier* pN = reinterpret_cast<Notifier*>(const_cast<void*>(pSender));
	std::cout << "onInsert, row:" << pN->getRow() << std::endl;
	++_insertCounter;
}


void SQLiteTest::onUpdate(const void* pSender)
{
	Notifier* pN = reinterpret_cast<Notifier*>(const_cast<void*>(pSender));
	std::cout << "onUpdate, row:" << pN->getRow() << std::endl;
	++_updateCounter;
}


void SQLiteTest::onDelete(const void* pSender)
{
	Notifier* pN = reinterpret_cast<Notifier*>(const_cast<void*>(pSender));
	std::cout << "onDelete, row:" << pN->getRow() << std::endl;
	++_deleteCounter;
}


void SQLiteTest::onCommit(const void* pSender)
{
	Notifier* pN = reinterpret_cast<Notifier*>(const_cast<void*>(pSender));
	std::cout << "onCommit, row:" << pN->getRow() << std::endl;
	++_commitCounter;
}


void SQLiteTest::onRollback(const void* pSender)
{
	Notifier* pN = reinterpret_cast<Notifier*>(const_cast<void*>(pSender));
	std::cout << "onRollback, row:" << pN->getRow() << std::endl;
	++_rollbackCounter;
}


void SQLiteTest::setTransactionIsolation(Session& session, Poco::UInt32 ti)
{
	if (session.hasTransactionIsolation(ti))
	{
		std::string funct = "setTransactionIsolation()";

		try 
		{
			Transaction t(session, false);
			t.setIsolation(ti);
			
			assert (ti == t.getIsolation());
			assert (t.isIsolation(ti));
			
			assert (ti == session.getTransactionIsolation());
			assert (session.isTransactionIsolation(ti));
		}
		catch(Poco::Exception& e){ std::cout << funct << ':' << e.displayText() << std::endl;}
	}
	else
	{
		std::cerr << '[' << name() << ']' << " Warning, transaction isolation not supported: ";
		switch (ti)
		{
		case Session::TRANSACTION_READ_COMMITTED:
			std::cerr << "READ COMMITTED"; break;
		case Session::TRANSACTION_READ_UNCOMMITTED:
			std::cerr << "READ UNCOMMITTED"; break;
		case Session::TRANSACTION_REPEATABLE_READ:
			std::cerr << "REPEATABLE READ"; break;
		case Session::TRANSACTION_SERIALIZABLE:
			std::cerr << "SERIALIZABLE"; break;
		default:
			std::cerr << "UNKNOWN"; break;
		}
		std::cerr << std::endl;
	}
}


void SQLiteTest::testSessionTransaction()
{
	Session session (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (session.isConnected());

	session << "DROP TABLE IF EXISTS Person", now;
	session << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	if (!session.canTransact())
	{
		std::cout << "Session not capable of transactions." << std::endl;
		return;
	}

	Session local (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (local.isConnected());
	
	try
	{
		local.setFeature("autoCommit", true);
		fail ("Setting SQLite auto-commit explicitly must fail!");
	}
	catch (NotImplementedException&) { }
	assert (local.getFeature("autoCommit"));

	std::string funct = "transaction()";
	std::vector<std::string> lastNames;
	std::vector<std::string> firstNames;
	std::vector<std::string> addresses;
	std::vector<int> ages;
	std::string tableName("Person");
	lastNames.push_back("LN1");
	lastNames.push_back("LN2");
	firstNames.push_back("FN1");
	firstNames.push_back("FN2");
	addresses.push_back("ADDR1");
	addresses.push_back("ADDR2");
	ages.push_back(1);
	ages.push_back(2);
	int count = 0, locCount = 0;
	std::string result;

	setTransactionIsolation(session, Session::TRANSACTION_READ_COMMITTED);

	session.begin();
	assert (!session.getFeature("autoCommit"));
	assert (session.isTransaction());
	session << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now;
	assert (session.isTransaction());

	Statement stmt = (local << "SELECT COUNT(*) FROM Person", into(locCount), async, now);

	session << "SELECT COUNT(*) FROM Person", into(count), now;
	assert (2 == count);
	assert (session.isTransaction());
	session.rollback();
	assert (!session.isTransaction());
	assert (session.getFeature("autoCommit"));

	stmt.wait();
	assert (0 == locCount);

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);
	assert (!session.isTransaction());

	session.begin();
	session << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now;
	assert (session.isTransaction());
	assert (!session.getFeature("autoCommit"));

	Statement stmt1 = (local << "SELECT COUNT(*) FROM Person", into(locCount), now);
	assert (0 == locCount);

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (2 == count);

	session.commit();
	assert (!session.isTransaction());
	assert (session.getFeature("autoCommit"));

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (2 == count);

	/* TODO: see http://www.sqlite.org/pragma.html#pragma_read_uncommitted
	setTransactionIsolation(session, Session::TRANSACTION_READ_UNCOMMITTED);
	*/

	session.close();
	assert (!session.isConnected());
	
	local.close();
	assert (!local.isConnected());
}


void SQLiteTest::testTransaction()
{
	Session session (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (session.isConnected());

	session << "DROP TABLE IF EXISTS Person", now;
	session << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	if (!session.canTransact())
	{
		std::cout << "Session not transaction-capable." << std::endl;
		return;
	}

	Session local(Poco::Data::SQLite::Connector::KEY, "dummy.db");

	setTransactionIsolation(session, Session::TRANSACTION_READ_COMMITTED);

	std::string funct = "transaction()";
	std::vector<std::string> lastNames;
	std::vector<std::string> firstNames;
	std::vector<std::string> addresses;
	std::vector<int> ages;
	std::string tableName("Person");
	lastNames.push_back("LN1");
	lastNames.push_back("LN2");
	firstNames.push_back("FN1");
	firstNames.push_back("FN2");
	addresses.push_back("ADDR1");
	addresses.push_back("ADDR2");
	ages.push_back(1);
	ages.push_back(2);
	int count = 0, locCount = 0;
	std::string result;

	session.setTransactionIsolation(Session::TRANSACTION_READ_COMMITTED);

	{
		Transaction trans(session);
		assert (trans.isActive());
		assert (session.isTransaction());
		
		session << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now;
		
		assert (session.isTransaction());
		assert (trans.isActive());

		session << "SELECT COUNT(*) FROM Person", into(count), now;
		assert (2 == count);
		assert (session.isTransaction());
		assert (trans.isActive());
		// no explicit commit, so transaction RAII must roll back here
	}
	assert (!session.isTransaction());

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);
	assert (!session.isTransaction());

	{
		Transaction trans(session);
		session << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now;

		Statement stmt1 = (local << "SELECT COUNT(*) FROM Person", into(locCount), now);

		assert (session.isTransaction());
		assert (trans.isActive());
		trans.commit();
		assert (!session.isTransaction());
		assert (!trans.isActive());
		assert (0 == locCount);
	}

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (2 == count);
	local << "SELECT count(*) FROM Person", into(count), now;
	assert (2 == count);

	session << "DELETE FROM Person", now;

	std::string sql1 = format("INSERT INTO Person VALUES ('%s','%s','%s',%d)", lastNames[0], firstNames[0], addresses[0], ages[0]);
	std::string sql2 = format("INSERT INTO Person VALUES ('%s','%s','%s',%d)", lastNames[1], firstNames[1], addresses[1], ages[1]);
	std::vector<std::string> sql;
	sql.push_back(sql1);
	sql.push_back(sql2);

	Transaction trans(session);

	trans.execute(sql1, false);
	session << "SELECT count(*) FROM Person", into(count), now;
	assert (1 == count);
	trans.execute(sql2, false);
	session << "SELECT count(*) FROM Person", into(count), now;
	assert (2 == count);

	Statement stmt2 = (local << "SELECT COUNT(*) FROM Person", into(locCount), now);
	assert (0 == locCount);

	trans.rollback();

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);

	trans.execute(sql);
	
	Statement stmt3 = (local << "SELECT COUNT(*) FROM Person", into(locCount), now);
	assert (2 == locCount);

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (2 == count);

	session.close();
	assert (!session.isConnected());
	
	local.close();
	assert (!local.isConnected());
}


struct TestCommitTransactor
{
	void operator () (Session& session) const
	{
		session << "INSERT INTO Person VALUES ('lastName','firstName','address',10)", now;
	}
};


struct TestRollbackTransactor
{
	void operator () (Session& session) const
	{
		session << "INSERT INTO Person VALUES ('lastName','firstName','address',10)", now;
		throw Poco::Exception("test");
	}
};


void SQLiteTest::testTransactor()
{
	Session session (Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert (session.isConnected());

	session << "DROP TABLE IF EXISTS Person", now;
	session << "CREATE TABLE IF NOT EXISTS Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))", now;

	std::string funct = "transaction()";
	int count = 0;

	assert (session.getFeature("autoCommit"));
	session.setTransactionIsolation(Session::TRANSACTION_READ_COMMITTED);

	TestCommitTransactor ct;
	Transaction t1(session, ct);

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (1 == count);

	session << "DELETE FROM Person", now;
	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);

	try
	{
		TestRollbackTransactor rt;
		Transaction t(session, rt);
		fail ("must fail");
	} catch (Poco::Exception&) { }

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);

	try
	{
		TestRollbackTransactor rt;
		Transaction t(session);
		t.transact(rt);
		fail ("must fail");
	} catch (Poco::Exception&) { }

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);

	try
	{
		TestRollbackTransactor rt;
		Transaction t(session, false);
		t.transact(rt);
		fail ("must fail");
	} catch (Poco::Exception&) { }

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);

	try
	{
		TestRollbackTransactor rt;
		Transaction t(session, true);
		t.transact(rt);
		fail ("must fail");
	} catch (Poco::Exception&) { }

	session << "SELECT count(*) FROM Person", into(count), now;
	assert (0 == count);

	session.close();
	assert (!session.isConnected());
}


void SQLiteTest::testFTS3()
{
#ifdef SQLITE_ENABLE_FTS3
	Session session(Poco::Data::SQLite::Connector::KEY, "dummy.db");
	assert(session.isConnected());

	session << "DROP TABLE IF EXISTS docs", now;
	session << "CREATE VIRTUAL TABLE docs USING fts3()", now;

	session << "INSERT INTO docs(docid, content) VALUES(1, 'a database is a software system')", now;
	session << "INSERT INTO docs(docid, content) VALUES(2, 'sqlite is a software system')", now;
	session << "INSERT INTO docs(docid, content) VALUES(3, 'sqlite is a database')", now;

	int docid = 0;
	session << "SELECT docid FROM docs WHERE docs MATCH 'sqlite AND database'", into(docid), now;
	assert(docid == 3);

	docid = 0;
	session << "SELECT docid FROM docs WHERE docs MATCH 'database sqlite'", into(docid), now;
	assert(docid == 3);

	std::vector<int> docids;
	session << "SELECT docid FROM docs WHERE docs MATCH 'sqlite OR database' ORDER BY docid", 
		into(docids), now;
	assert(docids.size() == 3);
	assert(docids[0] == 1);
	assert(docids[1] == 2);
	assert(docids[2] == 3);

	std::string content;
	docid = 0;
	session << "SELECT docid, content FROM docs WHERE docs MATCH 'database NOT sqlite'", 
		into(docid), into(content), now;
	assert(docid == 1);
	assert(content == "a database is a software system");

	docid = 0;
	session << "SELECT count(*) FROM docs WHERE docs MATCH 'database and sqlite'", into(docid), now;
	assert(docid == 0);
#else
	std::cout << "SQLite FTS not enabled, test not executed." << std::endl;
#endif // SQLITE_ENABLE_FTS3
}


void SQLiteTest::setUp()
{
}


void SQLiteTest::tearDown()
{
}


CppUnit::Test* SQLiteTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SQLiteTest");

	CppUnit_addTest(pSuite, SQLiteTest, testBinding);
	CppUnit_addTest(pSuite, SQLiteTest, testZeroRows);
	CppUnit_addTest(pSuite, SQLiteTest, testSimpleAccess);
	CppUnit_addTest(pSuite, SQLiteTest, testInMemory);
	CppUnit_addTest(pSuite, SQLiteTest, testNullCharPointer);
	CppUnit_addTest(pSuite, SQLiteTest, testInsertCharPointer);
	CppUnit_addTest(pSuite, SQLiteTest, testInsertCharPointer2);
	CppUnit_addTest(pSuite, SQLiteTest, testComplexType);
	CppUnit_addTest(pSuite, SQLiteTest, testSimpleAccessVector);
	CppUnit_addTest(pSuite, SQLiteTest, testComplexTypeVector);
	CppUnit_addTest(pSuite, SQLiteTest, testSharedPtrComplexTypeVector);
	CppUnit_addTest(pSuite, SQLiteTest, testInsertVector);
	CppUnit_addTest(pSuite, SQLiteTest, testInsertEmptyVector);
	CppUnit_addTest(pSuite, SQLiteTest, testAffectedRows);
	CppUnit_addTest(pSuite, SQLiteTest, testInsertSingleBulk);
	CppUnit_addTest(pSuite, SQLiteTest, testInsertSingleBulkVec);
	CppUnit_addTest(pSuite, SQLiteTest, testLimit);
	CppUnit_addTest(pSuite, SQLiteTest, testLimitOnce);
	CppUnit_addTest(pSuite, SQLiteTest, testLimitPrepare);
	CppUnit_addTest(pSuite, SQLiteTest, testLimitZero);
	CppUnit_addTest(pSuite, SQLiteTest, testPrepare);
	CppUnit_addTest(pSuite, SQLiteTest, testSetSimple);
	CppUnit_addTest(pSuite, SQLiteTest, testSetComplex);
	CppUnit_addTest(pSuite, SQLiteTest, testSetComplexUnique);
	CppUnit_addTest(pSuite, SQLiteTest, testMultiSetSimple);
	CppUnit_addTest(pSuite, SQLiteTest, testMultiSetComplex);
	CppUnit_addTest(pSuite, SQLiteTest, testMapComplex);
	CppUnit_addTest(pSuite, SQLiteTest, testMapComplexUnique);
	CppUnit_addTest(pSuite, SQLiteTest, testMultiMapComplex);
	CppUnit_addTest(pSuite, SQLiteTest, testSelectIntoSingle);
	CppUnit_addTest(pSuite, SQLiteTest, testSelectIntoSingleStep);
	CppUnit_addTest(pSuite, SQLiteTest, testSelectIntoSingleFail);
	CppUnit_addTest(pSuite, SQLiteTest, testLowerLimitOk);
	CppUnit_addTest(pSuite, SQLiteTest, testLowerLimitFail);
	CppUnit_addTest(pSuite, SQLiteTest, testCombinedLimits);
	CppUnit_addTest(pSuite, SQLiteTest, testCombinedIllegalLimits);
	CppUnit_addTest(pSuite, SQLiteTest, testRange);
	CppUnit_addTest(pSuite, SQLiteTest, testIllegalRange);
	CppUnit_addTest(pSuite, SQLiteTest, testSingleSelect);
	CppUnit_addTest(pSuite, SQLiteTest, testEmptyDB);
	CppUnit_addTest(pSuite, SQLiteTest, testCLOB);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple10);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector10);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple9);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector9);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple8);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector8);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple7);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector7);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple6);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector6);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple5);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector5);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple4);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector4);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple3);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector3);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple2);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector2);
	CppUnit_addTest(pSuite, SQLiteTest, testTuple1);
	CppUnit_addTest(pSuite, SQLiteTest, testTupleVector1);
	CppUnit_addTest(pSuite, SQLiteTest, testDateTime);
	CppUnit_addTest(pSuite, SQLiteTest, testInternalExtraction);
	CppUnit_addTest(pSuite, SQLiteTest, testPrimaryKeyConstraint);
	CppUnit_addTest(pSuite, SQLiteTest, testNullable);
	CppUnit_addTest(pSuite, SQLiteTest, testNulls);
	CppUnit_addTest(pSuite, SQLiteTest, testRowIterator);
	CppUnit_addTest(pSuite, SQLiteTest, testAsync);
	CppUnit_addTest(pSuite, SQLiteTest, testAny);
	CppUnit_addTest(pSuite, SQLiteTest, testDynamicAny);
	CppUnit_addTest(pSuite, SQLiteTest, testSQLChannel);
	CppUnit_addTest(pSuite, SQLiteTest, testSQLLogger);
	CppUnit_addTest(pSuite, SQLiteTest, testExternalBindingAndExtraction);
	CppUnit_addTest(pSuite, SQLiteTest, testBindingCount);
	CppUnit_addTest(pSuite, SQLiteTest, testMultipleResults);
	CppUnit_addTest(pSuite, SQLiteTest, testPair);
	CppUnit_addTest(pSuite, SQLiteTest, testReconnect);
	CppUnit_addTest(pSuite, SQLiteTest, testThreadModes);
	CppUnit_addTest(pSuite, SQLiteTest, testUpdateCallback);
	CppUnit_addTest(pSuite, SQLiteTest, testCommitCallback);
	CppUnit_addTest(pSuite, SQLiteTest, testRollbackCallback);
	CppUnit_addTest(pSuite, SQLiteTest, testNotifier);
	CppUnit_addTest(pSuite, SQLiteTest, testSessionTransaction);
	CppUnit_addTest(pSuite, SQLiteTest, testTransaction);
	CppUnit_addTest(pSuite, SQLiteTest, testTransactor);
	CppUnit_addTest(pSuite, SQLiteTest, testFTS3);

	return pSuite;
}
