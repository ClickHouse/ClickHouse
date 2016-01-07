//
// SQLExecutor.cpp
//
// $Id: //poco/1.4/Data/MySQL/testsuite/src/SQLExecutor.cpp#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CppUnit/TestCase.h"
#include "SQLExecutor.h"
#include "Poco/String.h"
#include "Poco/Format.h"
#include "Poco/Tuple.h"
#include "Poco/DateTime.h"
#include "Poco/Any.h"
#include "Poco/Exception.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/StatementImpl.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/Transaction.h"
#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/MySQL/MySQLException.h"

#ifdef _WIN32
#include <Winsock2.h>
#endif 

#include <mysql.h>
#include <iostream>
#include <limits>


using namespace Poco::Data;
using namespace Poco::Data::Keywords;
using Poco::Data::MySQL::ConnectionException;
using Poco::Data::MySQL::StatementException;
using Poco::format;
using Poco::Tuple;
using Poco::DateTime;
using Poco::Any;
using Poco::AnyCast;
using Poco::NotFoundException;
using Poco::InvalidAccessException;
using Poco::BadCastException;
using Poco::RangeException;


struct Person
{
	std::string lastName;
	std::string firstName;
	std::string address;
	int age;
	Person(){age = 0;}
	Person(const std::string& ln, const std::string& fn, const std::string& adr, int a):lastName(ln), firstName(fn), address(adr), age(a)
	{
	}
	bool operator==(const Person& other) const
	{
		return lastName == other.lastName && firstName == other.firstName && address == other.address && age == other.age;
	}

	bool operator < (const Person& p) const
	{
		if (age < p.age)
			return true;
		if (lastName < p.lastName)
			return true;
		if (firstName < p.firstName)
			return true;
		return (address < p.address);
	}

	const std::string& operator () () const
		/// This method is required so we can extract data to a map!
	{
		// we choose the lastName as examplary key
		return lastName;
	}
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
		pBinder->bind(pos++, obj.lastName, dir);
		pBinder->bind(pos++, obj.firstName, dir);
		pBinder->bind(pos++, obj.address, dir);
		pBinder->bind(pos++, obj.age, dir);
	}

	static void prepare(std::size_t pos, const Person& obj, AbstractPreparator::Ptr pPrepare)
	{
		// the table is defined as Person (LastName VARCHAR(30), FirstName VARCHAR, Address VARCHAR, Age INTEGER(3))
		poco_assert_dbg (!pPrepare.isNull());
		pPrepare->prepare(pos++, obj.lastName);
		pPrepare->prepare(pos++, obj.firstName);
		pPrepare->prepare(pos++, obj.address);
		pPrepare->prepare(pos++, obj.age);
	}

	static std::size_t size()
	{
		return 4;
	}

	static void extract(std::size_t pos, Person& obj, const Person& defVal, AbstractExtractor::Ptr pExt)
	{
		poco_assert_dbg (!pExt.isNull());
		if (!pExt->extract(pos++, obj.lastName))
			obj.lastName = defVal.lastName;
		if (!pExt->extract(pos++, obj.firstName))
			obj.firstName = defVal.firstName;
		if (!pExt->extract(pos++, obj.address))
			obj.address = defVal.address;
		if (!pExt->extract(pos++, obj.age))
			obj.age = defVal.age;
	}

private:
	TypeHandler();
	~TypeHandler();
	TypeHandler(const TypeHandler&);
	TypeHandler& operator=(const TypeHandler&);
};


} } // namespace Poco::Data


SQLExecutor::SQLExecutor(const std::string& name, Poco::Data::Session* pSession): 
	CppUnit::TestCase(name),
	_pSession(pSession)
{
}


SQLExecutor::~SQLExecutor()
{
}


void SQLExecutor::bareboneMySQLTest(const char* host, const char* user, const char* pwd, const char* db, int port, const char* tableCreateString)
{
	int rc;
	MYSQL* hsession = mysql_init(0);
	assert (hsession != 0);

	MYSQL* tmp = mysql_real_connect(hsession, host, user, pwd, db, port, 0, 0);
	assert(tmp == hsession);
	
	MYSQL_STMT* hstmt = mysql_stmt_init(hsession);
	assert(hstmt != 0);
	
	std::string sql = "DROP TABLE Test";
	mysql_real_query(hsession, sql.c_str(), static_cast<unsigned long>(sql.length()));
	
	sql = tableCreateString;
	rc = mysql_stmt_prepare(hstmt, sql.c_str(), static_cast<unsigned long>(sql.length())); 
	assert(rc == 0);

	rc = mysql_stmt_execute(hstmt);
	assert(rc == 0);

	sql = "INSERT INTO Test VALUES (?,?,?,?,?)";
	rc = mysql_stmt_prepare(hstmt, sql.c_str(), static_cast<unsigned long>(sql.length())); 
	assert(rc == 0);

	std::string str[3] = { "111", "222", "333" };
	int fourth = 4;
	float fifth = 1.5;

	MYSQL_BIND bind_param[5] = {{0}};

	bind_param[0].buffer		= const_cast<char*>(str[0].c_str());
	bind_param[0].buffer_length = static_cast<unsigned long>(str[0].length());
	bind_param[0].buffer_type   = MYSQL_TYPE_STRING;

	bind_param[1].buffer		= const_cast<char*>(str[1].c_str());
	bind_param[1].buffer_length = static_cast<unsigned long>(str[1].length());
	bind_param[1].buffer_type   = MYSQL_TYPE_STRING;

	bind_param[2].buffer		= const_cast<char*>(str[2].c_str());
	bind_param[2].buffer_length = static_cast<unsigned long>(str[2].length());
	bind_param[2].buffer_type   = MYSQL_TYPE_STRING;

	bind_param[3].buffer		= &fourth;
	bind_param[3].buffer_type   = MYSQL_TYPE_LONG;

	bind_param[4].buffer		= &fifth;
	bind_param[4].buffer_type   = MYSQL_TYPE_FLOAT;

	rc = mysql_stmt_bind_param(hstmt, bind_param);
	assert (rc == 0);

	rc = mysql_stmt_execute(hstmt);
	assert (rc == 0);

	sql = "SELECT * FROM Test";
	rc = mysql_stmt_prepare(hstmt, sql.c_str(), static_cast<unsigned long>(sql.length()));
	assert (rc == 0);

	char chr[3][5] = {{ 0 }};
	unsigned long lengths[5] = { 0 };
	fourth = 0;
	fifth = 0.0f;

	MYSQL_BIND bind_result[5] = {{0}};
	
	bind_result[0].buffer		= chr[0];
	bind_result[0].buffer_length = sizeof(chr[0]);
	bind_result[0].buffer_type   = MYSQL_TYPE_STRING;
	bind_result[0].length		= &lengths[0];

	bind_result[1].buffer		= chr[1];
	bind_result[1].buffer_length = sizeof(chr[1]);
	bind_result[1].buffer_type   = MYSQL_TYPE_STRING;
	bind_result[1].length		= &lengths[1];

	bind_result[2].buffer		= chr[2];
	bind_result[2].buffer_length = sizeof(chr[2]);
	bind_result[2].buffer_type   = MYSQL_TYPE_STRING;
	bind_result[2].length		= &lengths[2];

	bind_result[3].buffer		= &fourth;
	bind_result[3].buffer_type   = MYSQL_TYPE_LONG;
	bind_result[3].length		= &lengths[3];

	bind_result[4].buffer		= &fifth;
	bind_result[4].buffer_type   = MYSQL_TYPE_FLOAT;
	bind_result[4].length		= &lengths[4];

	rc = mysql_stmt_bind_result(hstmt, bind_result);
	assert (rc == 0);

	rc = mysql_stmt_execute(hstmt);
	assert (rc == 0);
	rc = mysql_stmt_fetch(hstmt);
	assert (rc == 0);

			assert (0 == std::strncmp("111", chr[0], 3));
			assert (0 == std::strncmp("222", chr[1], 3));
			assert (0 == std::strncmp("333", chr[2], 3));
			assert (4 == fourth);
			assert (1.5 == fifth);

	rc = mysql_stmt_close(hstmt);
	assert(rc == 0);

	sql = "DROP TABLE Test";
	rc = mysql_real_query(hsession, sql.c_str(), static_cast<unsigned long>(sql.length()));
	assert(rc == 0);

	mysql_close(hsession);
}


void SQLExecutor::simpleAccess()
{
	std::string funct = "simpleAccess()";
	std::string lastName = "lastName";
	std::string firstName("firstName");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;

	count = 0;
	try 
	{ 
		Statement stmt(*_pSession);
		stmt << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(age);//, now;  
		stmt.execute();
	}
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now;  }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	assert (count == 1);

	try { *_pSession << "SELECT LastName FROM Person", into(result), now;  }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (lastName == result);

	try { *_pSession << "SELECT Age FROM Person", into(count), now;  }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == age);
}


void SQLExecutor::complexType()
{
	std::string funct = "complexType()";
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(p1), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(p2), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	Person c1;
	Person c2;
	try { *_pSession << "SELECT * FROM Person WHERE LastName = 'LN1'", into(c1), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (c1 == p1);
}


void SQLExecutor::simpleAccessVector()
{
	std::string funct = "simpleAccessVector()";
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

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	std::vector<std::string> lastNamesR;
	std::vector<std::string> firstNamesR;
	std::vector<std::string> addressesR;
	std::vector<int> agesR;
	try { *_pSession << "SELECT * FROM Person", into(lastNamesR), into(firstNamesR), into(addressesR), into(agesR), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ages == agesR);
	assert (lastNames == lastNamesR);
	assert (firstNames == firstNamesR);
	assert (addresses == addressesR);
}


void SQLExecutor::complexTypeVector()
{
	std::string funct = "complexTypeVector()";
	std::vector<Person> people;
	people.push_back(Person("LN1", "FN1", "ADDR1", 1));
	people.push_back(Person("LN2", "FN2", "ADDR2", 2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	std::vector<Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result == people);
}


void SQLExecutor::insertVector()
{
	std::string funct = "insertVector()";
	std::vector<std::string> str;
	str.push_back("s1");
	str.push_back("s2");
	str.push_back("s3");
	str.push_back("s3");
	int count = 100;

	{
		Statement stmt((*_pSession << "INSERT INTO Strings VALUES (?)", use(str)));
		try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
		catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
		catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
		assert (count == 0);

		try { stmt.execute(); }
		catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

		try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
		catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
		catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
		assert (count == 4);
	}
	count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 4);
}


void SQLExecutor::insertEmptyVector()
{
	std::string funct = "insertEmptyVector()";
	std::vector<std::string> str;

	try
	{
		*_pSession << "INSERT INTO Strings VALUES (?)", use(str), now;
		fail("empty collections should not work");
	}
	catch (Poco::Exception&)
	{
	}
}


void SQLExecutor::insertSingleBulk()
{
	std::string funct = "insertSingleBulk()";
	int x = 0;
	Statement stmt((*_pSession << "INSERT INTO Strings VALUES (?)", use(x)));

	for (x = 0; x < 100; ++x)
	{
		std::size_t i = stmt.execute();
		assert (i == 1);
	}

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 100);

	try { *_pSession << "SELECT SUM(str) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == ((0+99)*100/2));
}


void SQLExecutor::unsignedInts()
{
	std::string funct = "unsignedInts()";
	Poco::UInt32 data = std::numeric_limits<Poco::UInt32>::max();
	Poco::UInt32 ret = 0;

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	try { *_pSession << "SELECT str FROM Strings", into(ret), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ret == data);
}


void SQLExecutor::floats()
{
	std::string funct = "floats()";
	float data = 1.5f;
	float ret = 0.0f;

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	try { *_pSession << "SELECT str FROM Strings", into(ret), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ret == data);
}


void SQLExecutor::doubles()
{
	std::string funct = "floats()";
	double data = 1.5;
	double ret = 0.0;

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	try { *_pSession << "SELECT str FROM Strings", into(ret), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ret == data);
}


void SQLExecutor::insertSingleBulkVec()
{
	std::string funct = "insertSingleBulkVec()";
	std::vector<int> data;
	
	for (int x = 0; x < 100; ++x)
		data.push_back(x);

	Statement stmt((*_pSession << "INSERT INTO Strings VALUES (?)", use(data)));
	stmt.execute();

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	assert (count == 100);
	try { *_pSession << "SELECT SUM(str) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == ((0+99)*100/2));
}


void SQLExecutor::limits()
{
	std::string funct = "limit()";
	std::vector<int> data;
	for (int x = 0; x < 100; ++x)
	{
		data.push_back(x);
	}

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	std::vector<int> retData;
	try { *_pSession << "SELECT * FROM Strings", into(retData), limit(50), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (retData.size() == 50);
	for (int x = 0; x < 50; ++x)
	{
		assert(data[x] == retData[x]);
	}
}


void SQLExecutor::limitZero()
{
	std::string funct = "limitZero()";
	std::vector<int> data;
	for (int x = 0; x < 100; ++x)
	{
		data.push_back(x);
	}

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	std::vector<int> retData;
	try { *_pSession << "SELECT * FROM Strings", into(retData), limit(0), now; }// stupid test, but at least we shouldn't crash
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (retData.size() == 0);
}


void SQLExecutor::limitOnce()
{
	std::string funct = "limitOnce()";
	std::vector<int> data;
	for (int x = 0; x < 101; ++x)
	{
		data.push_back(x);
	}

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	std::vector<int> retData;
	Statement stmt = (*_pSession << "SELECT * FROM Strings", into(retData), limit(50), now);
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


void SQLExecutor::limitPrepare()
{
	std::string funct = "limitPrepare()";
	std::vector<int> data;
	for (int x = 0; x < 100; ++x)
	{
		data.push_back(x);
	}

	try { *_pSession << "INSERT INTO Strings VALUES (?)", use(data), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	std::vector<int> retData;
	Statement stmt = (*_pSession << "SELECT * FROM Strings", into(retData), limit(50));
	assert (retData.size() == 0);
	assert (!stmt.done());

	try { stmt.execute(); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (!stmt.done());
	assert (retData.size() == 50);

	try { stmt.execute(); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (stmt.done());
	assert (retData.size() == 100);

	try { stmt.execute(); }// will restart execution!
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (!stmt.done());
	assert (retData.size() == 150);
	for (int x = 0; x < 150; ++x)
	{
		assert(data[x%100] == retData[x]);
	}
}



void SQLExecutor::prepare()
{
	std::string funct = "prepare()";
	std::vector<int> data;
	for (int x = 0; x < 100; x += 2)
	{
		data.push_back(x);
	}

	{
		Statement stmt((*_pSession << "INSERT INTO Strings VALUES (?)", use(data)));
	}
	// stmt should not have been executed when destroyed
	int count = 100;
	try { *_pSession << "SELECT COUNT(*) FROM Strings", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 0);
}


void SQLExecutor::setSimple()
{
	std::string funct = "setSimple()";
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

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	std::set<std::string> lastNamesR;
	std::set<std::string> firstNamesR;
	std::set<std::string> addressesR;
	std::set<int> agesR;
	try { *_pSession << "SELECT * FROM Person", into(lastNamesR), into(firstNamesR), into(addressesR), into(agesR), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ages == agesR);
	assert (lastNames == lastNamesR);
	assert (firstNames == firstNamesR);
	assert (addresses == addressesR);
}


void SQLExecutor::setComplex()
{
	std::string funct = "setComplex()";
	std::set<Person> people;
	people.insert(Person("LN1", "FN1", "ADDR1", 1));
	people.insert(Person("LN2", "FN2", "ADDR2", 2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	std::set<Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result == people);
}


void SQLExecutor::setComplexUnique()
{
	std::string funct = "setComplexUnique()";
	std::vector<Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	people.push_back(p1);
	people.push_back(p1);
	people.push_back(p1);
	people.push_back(p1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.push_back(p2);

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 5);

	std::set<Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result.size() == 2);
	assert (*result.begin() == p1);
	assert (*++result.begin() == p2);
}

void SQLExecutor::multiSetSimple()
{
	std::string funct = "multiSetSimple()";
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

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	std::multiset<std::string> lastNamesR;
	std::multiset<std::string> firstNamesR;
	std::multiset<std::string> addressesR;
	std::multiset<int> agesR;
	try { *_pSession << "SELECT * FROM Person", into(lastNamesR), into(firstNamesR), into(addressesR), into(agesR), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ages.size() == agesR.size());
	assert (lastNames.size() == lastNamesR.size());
	assert (firstNames.size() == firstNamesR.size());
	assert (addresses.size() == addressesR.size());
}


void SQLExecutor::multiSetComplex()
{
	std::string funct = "multiSetComplex()";
	std::multiset<Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	people.insert(p1);
	people.insert(p1);
	people.insert(p1);
	people.insert(p1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(p2);

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 5);

	std::multiset<Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result.size() == people.size());
}


void SQLExecutor::mapComplex()
{
	std::string funct = "mapComplex()";
	std::map<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN2", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);

	std::map<std::string, Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result == people);
}


void SQLExecutor::mapComplexUnique()
{
	std::string funct = "mapComplexUnique()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN2", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 5);

	std::map<std::string, Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result.size() == 2);
}


void SQLExecutor::multiMapComplex()
{
	std::string funct = "multiMapComplex()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN2", p2));
	
	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 5);

	std::multimap<std::string, Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result.size() == people.size());
}


void SQLExecutor::selectIntoSingle()
{
	std::string funct = "selectIntoSingle()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	try { *_pSession << "SELECT * FROM Person", into(result), limit(1), now; }// will return 1 object into one single result
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result == p1);
}


void SQLExecutor::selectIntoSingleStep()
{
	std::string funct = "selectIntoSingleStep()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	Statement stmt = (*_pSession << "SELECT * FROM Person", into(result), limit(1)); 
	stmt.execute();
	assert (result == p1);
	assert (!stmt.done());
	stmt.execute();
	assert (result == p2);
	assert (stmt.done());
}


void SQLExecutor::selectIntoSingleFail()
{
	std::string funct = "selectIntoSingleFail()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), limit(2, true), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	try
	{
		*_pSession << "SELECT * FROM Person", into(result), limit(1, true), now; // will fail now
		fail("hardLimit is set: must fail");
	}
	catch(Poco::Data::LimitException&)
	{
	}
}


void SQLExecutor::lowerLimitOk()
{
	std::string funct = "lowerLimitOk()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	try
	{
		*_pSession << "SELECT * FROM Person", into(result), lowerLimit(2), now; // will return 2 objects into one single result but only room for one!
		fail("Not enough space for results");
	}
	catch(Poco::Exception&)
	{
	}
}


void SQLExecutor::singleSelect()
{
	std::string funct = "singleSelect()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	Statement stmt = (*_pSession << "SELECT * FROM Person", into(result), limit(1));
	stmt.execute();
	assert (result == p1);
	assert (!stmt.done());
	stmt.execute();
	assert (result == p2);
	assert (stmt.done());
}


void SQLExecutor::lowerLimitFail()
{
	std::string funct = "lowerLimitFail()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	try
	{
		*_pSession << "SELECT * FROM Person", into(result), lowerLimit(3), now; // will fail
		fail("should fail. not enough data");
	}
	catch(Poco::Exception&)
	{
	}
}


void SQLExecutor::combinedLimits()
{
	std::string funct = "combinedLimits()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	std::vector <Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), lowerLimit(2), upperLimit(2), now; }// will return 2 objects
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result.size() == 2);
	assert (result[0] == p1);
	assert (result[1] == p2);
}



void SQLExecutor::ranges()
{
	std::string funct = "range()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	std::vector <Person> result;
	try { *_pSession << "SELECT * FROM Person", into(result), range(2, 2), now; }// will return 2 objects
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (result.size() == 2);
	assert (result[0] == p1);
	assert (result[1] == p2);
}


void SQLExecutor::combinedIllegalLimits()
{
	std::string funct = "combinedIllegalLimits()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	try
	{
		*_pSession << "SELECT * FROM Person", into(result), lowerLimit(3), upperLimit(2), now;
		fail("lower > upper is not allowed");
	}
	catch(LimitException&)
	{
	}
}


void SQLExecutor::illegalRange()
{
	std::string funct = "illegalRange()";
	std::multimap<std::string, Person> people;
	Person p1("LN1", "FN1", "ADDR1", 1);
	Person p2("LN2", "FN2", "ADDR2", 2);
	people.insert(std::make_pair("LN1", p1));
	people.insert(std::make_pair("LN1", p2));

	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(people), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 2);
	Person result;
	try
	{
		*_pSession << "SELECT * FROM Person", into(result), range(3, 2), now;
		fail("lower > upper is not allowed");
	}
	catch(LimitException&)
	{
	}
}


void SQLExecutor::emptyDB()
{
	std::string funct = "emptyDB()";
	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 0);

	Person result;
	Statement stmt = (*_pSession << "SELECT * FROM Person", into(result), limit(1));
	stmt.execute();
	assert (result.firstName.empty());
	assert (stmt.done());
}


void SQLExecutor::dateTime()
{
	std::string funct = "dateTime()";
	std::string lastName("Bart");
	std::string firstName("Simpson");
	std::string address("Springfield");
	DateTime birthday(1980, 4, 1, 5, 45, 12);
	
	int count = 0;
	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(birthday), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);
	
	DateTime bd;
	assert (bd != birthday);
	try { *_pSession << "SELECT Birthday FROM Person", into(bd), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (bd == birthday);
	
	std::cout << std::endl << RecordSet(*_pSession, "SELECT * FROM Person");
}


void SQLExecutor::date()
{
	std::string funct = "date()";
	std::string lastName("Bart");
	std::string firstName("Simpson");
	std::string address("Springfield");
	Date birthday(1980, 4, 1);
	
	int count = 0;
	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(birthday), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);
	
	Date bd;
	assert (bd != birthday);
	try { *_pSession << "SELECT Birthday FROM Person", into(bd), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (bd == birthday);
	
	std::cout << std::endl << RecordSet(*_pSession, "SELECT * FROM Person");
}


void SQLExecutor::time()
{
	std::string funct = "date()";
	std::string lastName("Bart");
	std::string firstName("Simpson");
	std::string address("Springfield");
	Time birthday(1, 2, 3);
	
	int count = 0;
	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(birthday), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);
	
	Time bd;
	assert (bd != birthday);
	try { *_pSession << "SELECT Birthday FROM Person", into(bd), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (bd == birthday);
	
	std::cout << std::endl << RecordSet(*_pSession, "SELECT * FROM Person");
}


void SQLExecutor::blob(unsigned int bigSize)
{
	std::string funct = "blob()";
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");

	Poco::Data::CLOB img("0123456789", 10);
	int count = 0;
	try { *_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(img), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	Poco::Data::CLOB res;
	assert (res.size() == 0);
	try { *_pSession << "SELECT Image FROM Person", into(res), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (res == img);

	Poco::Data::CLOB big;
	std::vector<char> v(bigSize, 'x');
	big.assignRaw(&v[0], (std::size_t) v.size());

	assert (big.size() == (std::size_t) bigSize);

	try { *_pSession << "DELETE FROM Person", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	try { *_pSession << "INSERT INTO Person VALUES(?,?,?,?)", use(lastName), use(firstName), use(address), use(big), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	try { *_pSession << "SELECT Image FROM Person", into(res), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (res == big);
}


void SQLExecutor::blobStmt()
{
	std::string funct = "blobStmt()";
	std::string lastName("lastname");
	std::string firstName("firstname");
	std::string address("Address");
	Poco::Data::CLOB blob("0123456789", 10);

	int count = 0;
	Statement ins = (*_pSession << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(blob));
	ins.execute();
	try { *_pSession << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	Poco::Data::CLOB res;
	poco_assert (res.size() == 0);
	Statement stmt = (*_pSession << "SELECT Image FROM Person", into(res));
	try { stmt.execute(); }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	poco_assert (res == blob);
}


void SQLExecutor::tuples()
{
	typedef Tuple<int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int> TupleType;
	std::string funct = "tuples()";
	TupleType t(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19);

	try { *_pSession << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", use(t), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	TupleType ret(-10,-11,-12,-13,-14,-15,-16,-17,-18,-19);
	assert (ret != t);
	try { *_pSession << "SELECT * FROM Tuples", into(ret), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ret == t);
}


void SQLExecutor::tupleVector()
{
	typedef Tuple<int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int> TupleType;
	std::string funct = "tupleVector()";
	TupleType t(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19);
	Tuple<int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int> 
		t10(10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29);
	TupleType t100(100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119);
	std::vector<TupleType> v;
	v.push_back(t);
	v.push_back(t10);
	v.push_back(t100);

	try { *_pSession << "INSERT INTO Tuples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", use(v), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Tuples", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (v.size() == (std::size_t) count);

	std::vector<Tuple<int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int> > ret;
	try { *_pSession << "SELECT * FROM Tuples", into(ret), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (ret == v);
}


void SQLExecutor::internalExtraction()
{
	/*std::string funct = "internalExtraction()";
	std::vector<Tuple<int, double, std::string> > v;
	v.push_back(Tuple<int, double, std::string>(1, 1.5f, "3"));
	v.push_back(Tuple<int, double, std::string>(2, 2.5f, "4"));
	v.push_back(Tuple<int, double, std::string>(3, 3.5f, "5"));
	v.push_back(Tuple<int, double, std::string>(4, 4.5f, "6"));

	try { *_pSession << "INSERT INTO Vectors VALUES (?,?,?)", use(v), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	try 
	{ 
		Statement stmt = (*_pSession << "SELECT * FROM Vectors", now);
		RecordSet rset(stmt);

		assert (3 == rset.columnCount());
		assert (4 == rset.rowCount());

		int curVal = 3;
		do
		{
			assert (rset["str0"] == curVal);
			++curVal;
		} while (rset.moveNext());

		rset.moveFirst();
		assert (rset["str0"] == "3");
		rset.moveLast();
		assert (rset["str0"] == "6");

		RecordSet rset2(rset);
		assert (3 == rset2.columnCount());
		assert (4 == rset2.rowCount());

		int i = rset.value<int>(0,0);
		assert (1 == i);

		std::string s = rset.value(0,0);
		assert ("1" == s);

		int a = rset.value<int>(0,2);
		assert (3 == a);

		try
		{
			double d = rset.value<double>(1,1);
			assert (2.5 == d);
		}
		catch (BadCastException&)
		{
			float f = rset.value<float>(1,1);
			assert (2.5 == f);
		}

		s = rset.value<std::string>(2,2);
		assert ("5" == s);
		i = rset.value("str0", 2);
		assert (5 == i);
		
		const Column<int>& col = rset.column<int>(0);
		Column<int>::Iterator it = col.begin();
		Column<int>::Iterator end = col.end();
		for (int i = 1; it != end; ++it, ++i)
			assert (*it == i);

		rset = (*_pSession << "SELECT COUNT(*) AS cnt FROM Vectors", now);

		//various results for COUNT(*) are received from different drivers
		try
		{
			//this is what most drivers will return
			int i = rset.value<int>(0,0);
			assert (4 == i);
		}
		catch(BadCastException&)
		{
			try
			{
				//this is for Oracle
				double i = rset.value<double>(0,0);
				assert (4 == int(i));
			}
			catch(BadCastException&)
			{
				//this is for PostgreSQL
				Poco::Int64 big = rset.value<Poco::Int64>(0,0);
				assert (4 == big);
			}
		}

		s = rset.value("cnt", 0).convert<std::string>();
		assert ("4" == s);

		try { const Column<int>& col1 = rset.column<int>(100); fail ("must fail"); }
		catch (RangeException&) { }

		try	{ rset.value<std::string>(0,0); fail ("must fail"); }
		catch (BadCastException&) {	}
		
		stmt = (*_pSession << "DELETE FROM Vectors", now);
		rset = stmt;

		try { const Column<int>& col1 = rset.column<int>(0); fail ("must fail"); }
		catch (RangeException&) { }
	}
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
*/
}


void SQLExecutor::doNull()
{
	std::string funct = "null()";
	
	*_pSession << "INSERT INTO Vectors VALUES (?, ?, ?)", 
						use(Poco::Data::Keywords::null), 
						use(Poco::Data::Keywords::null), 
						use(Poco::Data::Keywords::null), now;

	int count = 0;
	try { *_pSession << "SELECT COUNT(*) FROM Vectors", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	int i0 = 0;
	Statement stmt1 = (*_pSession << "SELECT i0 FROM Vectors", into(i0, Poco::Data::Position(0), -1));
	try { stmt1.execute(); }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	poco_assert (i0 == -1);

	float flt0 = 0;
	Statement stmt2 = (*_pSession << "SELECT flt0 FROM Vectors", into(flt0, Poco::Data::Position(0), 3.25f));
	try { stmt2.execute(); }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	poco_assert (flt0 == 3.25);

	std::string str0("string");
	Statement stmt3 = (*_pSession << "SELECT str0 FROM Vectors", into(str0, Poco::Data::Position(0), std::string("DEFAULT")));
	try { stmt3.execute(); }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	poco_assert (str0 == "DEFAULT");
}


void SQLExecutor::setTransactionIsolation(Session& session, Poco::UInt32 ti)
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
		std::cout << "Transaction isolation not supported: ";
		switch (ti)
		{
		case Session::TRANSACTION_READ_COMMITTED:
			std::cout << "READ COMMITTED"; break;
		case Session::TRANSACTION_READ_UNCOMMITTED:
			std::cout << "READ UNCOMMITTED"; break;
		case Session::TRANSACTION_REPEATABLE_READ:
			std::cout << "REPEATABLE READ"; break;
		case Session::TRANSACTION_SERIALIZABLE:
			std::cout << "SERIALIZABLE"; break;
		default:
			std::cout << "UNKNOWN"; break;
		}
		std::cout << std::endl;
	}
}


void SQLExecutor::sessionTransaction(const std::string& connect)
{
	if (!_pSession->canTransact())
	{
		std::cout << "Session not capable of transactions." << std::endl;
		return;
	}

	Session local("mysql", connect);
	local.setFeature("autoCommit", true);

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

	bool autoCommit = _pSession->getFeature("autoCommit");

	_pSession->setFeature("autoCommit", true);
	assert (!_pSession->isTransaction());
	_pSession->setFeature("autoCommit", false);
	assert (!_pSession->isTransaction());

	setTransactionIsolation((*_pSession), Session::TRANSACTION_READ_UNCOMMITTED);
	setTransactionIsolation((*_pSession), Session::TRANSACTION_REPEATABLE_READ);
	setTransactionIsolation((*_pSession), Session::TRANSACTION_SERIALIZABLE);

	setTransactionIsolation((*_pSession), Session::TRANSACTION_READ_COMMITTED);

	_pSession->begin();
	assert (_pSession->isTransaction());
	try { (*_pSession) << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (_pSession->isTransaction());

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (0 == locCount);

	try { (*_pSession) << "SELECT COUNT(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (2 == count);
	assert (_pSession->isTransaction());
	_pSession->rollback();
	assert (!_pSession->isTransaction());

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (0 == locCount);

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (0 == count);
	assert (!_pSession->isTransaction());

	_pSession->begin();
	try { (*_pSession) << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (_pSession->isTransaction());

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (0 == locCount);

	_pSession->commit();
	assert (!_pSession->isTransaction());

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (2 == locCount);

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (2 == count);

	_pSession->setFeature("autoCommit", autoCommit);
}


void SQLExecutor::transaction(const std::string& connect)
{
	if (!_pSession->canTransact())
	{
		std::cout << "Session not transaction-capable." << std::endl;
		return;
	}

	Session local("mysql", connect);
	local.setFeature("autoCommit", true);

	setTransactionIsolation((*_pSession), Session::TRANSACTION_READ_COMMITTED);
	setTransactionIsolation(local, Session::TRANSACTION_READ_COMMITTED);

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

	bool autoCommit = _pSession->getFeature("autoCommit");

	_pSession->setFeature("autoCommit", true);
	assert (!_pSession->isTransaction());
	_pSession->setFeature("autoCommit", false);
	assert (!_pSession->isTransaction());
	_pSession->setTransactionIsolation(Session::TRANSACTION_READ_COMMITTED);

	{
		Transaction trans((*_pSession));
		assert (trans.isActive());
		assert (_pSession->isTransaction());
		
		try { (*_pSession) << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
		catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
		catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
		
		assert (_pSession->isTransaction());
		assert (trans.isActive());

		try { (*_pSession) << "SELECT COUNT(*) FROM Person", into(count), now; }
		catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
		catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
		assert (2 == count);
		assert (_pSession->isTransaction());
		assert (trans.isActive());
	}
	assert (!_pSession->isTransaction());

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (0 == count);
	assert (!_pSession->isTransaction());

	{
		Transaction trans((*_pSession));
		try { (*_pSession) << "INSERT INTO Person VALUES (?,?,?,?)", use(lastNames), use(firstNames), use(addresses), use(ages), now; }
		catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
		catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

		local << "SELECT COUNT(*) FROM Person", into(locCount), now;
		assert (0 == locCount);

		assert (_pSession->isTransaction());
		assert (trans.isActive());
		trans.commit();
		assert (!_pSession->isTransaction());
		assert (!trans.isActive());
		local << "SELECT COUNT(*) FROM Person", into(locCount), now;
		assert (2 == locCount);
	}

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (2 == count);

	_pSession->begin();
	try { (*_pSession) << "DELETE FROM Person", now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (2 == locCount);

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (0 == count);
	_pSession->commit();

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (0 == locCount);

	std::string sql1 = format("INSERT INTO Person VALUES ('%s','%s','%s',%d)", lastNames[0], firstNames[0], addresses[0], ages[0]);
	std::string sql2 = format("INSERT INTO Person VALUES ('%s','%s','%s',%d)", lastNames[1], firstNames[1], addresses[1], ages[1]);
	std::vector<std::string> sql;
	sql.push_back(sql1);
	sql.push_back(sql2);

	Transaction trans((*_pSession));

	trans.execute(sql1, false);
	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (1 == count);
	trans.execute(sql2, false);
	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (2 == count);

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (0 == locCount);

	trans.rollback();

	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (0 == locCount);

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (0 == count);

	trans.execute(sql);
	
	local << "SELECT COUNT(*) FROM Person", into(locCount), now;
	assert (2 == locCount);

	try { (*_pSession) << "SELECT count(*) FROM Person", into(count), now; }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (2 == count);

	_pSession->setFeature("autoCommit", autoCommit);
}


void SQLExecutor::reconnect()
{
	std::string funct = "reconnect()";
	std::string lastName = "lastName";
	std::string firstName("firstName");
	std::string address("Address");
	int age = 133132;
	int count = 0;
	std::string result;

	try { (*_pSession) << "INSERT INTO Person VALUES (?,?,?,?)", use(lastName), use(firstName), use(address), use(age), now;  }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }

	count = 0;
	try { (*_pSession) << "SELECT COUNT(*) FROM Person", into(count), now;  }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == 1);

	assert (_pSession->isConnected());
	_pSession->close();
	assert (!_pSession->isConnected());
	try 
	{
		(*_pSession) << "SELECT LastName FROM Person", into(result), now;  
		fail ("must fail");
	}
	catch(NotConnectedException&){ }
	assert (!_pSession->isConnected());

	_pSession->open();
	assert (_pSession->isConnected());
	try { (*_pSession) << "SELECT Age FROM Person", into(count), now;  }
	catch(ConnectionException& ce){ std::cout << ce.displayText() << std::endl; fail (funct); }
	catch(StatementException& se){ std::cout << se.displayText() << std::endl; fail (funct); }
	assert (count == age);
	assert (_pSession->isConnected());
}
