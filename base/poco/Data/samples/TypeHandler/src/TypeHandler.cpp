//
// Binding.cpp
//
// This sample demonstrates the Data library.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0


#include "Poco/SharedPtr.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/TypeHandler.h"
#include "Poco/Data/SQLite/Connector.h"
#include <vector>
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::DateTime;
using Poco::DateTimeFormatter;
using Poco::Data::Session;
using Poco::Data::Statement;


struct Person
{
	std::string   name;
	std::string   address;
	int           age;
	DateTime birthday;
};


namespace Poco {
namespace Data {


template <>
class TypeHandler<Person>
	/// Defining a specialization of TypeHandler for Person allows us
	/// to use the Person struct in use and into clauses.
{
public:
	static std::size_t size()
	{
		return 4;
	}
	
	static void bind(std::size_t pos, const Person& person, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
	{
		TypeHandler<std::string>::bind(pos++, person.name, pBinder, dir);
		TypeHandler<std::string>::bind(pos++, person.address, pBinder, dir);
		TypeHandler<int>::bind(pos++, person.age, pBinder, dir);
		TypeHandler<DateTime>::bind(pos++, person.birthday, pBinder, dir);
	}
	
	static void extract(std::size_t pos, Person& person, const Person& deflt, AbstractExtractor::Ptr pExtr)
	{
		TypeHandler<std::string>::extract(pos++, person.name, deflt.name, pExtr);
		TypeHandler<std::string>::extract(pos++, person.address, deflt.address, pExtr);
		TypeHandler<int>::extract(pos++, person.age, deflt.age, pExtr);
		TypeHandler<DateTime>::extract(pos++, person.birthday, deflt.birthday, pExtr);
	}
	
	static void prepare(std::size_t pos, const Person& person, AbstractPreparator::Ptr pPrep)
	{
		TypeHandler<std::string>::prepare(pos++, person.name, pPrep);
		TypeHandler<std::string>::prepare(pos++, person.address, pPrep);
		TypeHandler<int>::prepare(pos++, person.age, pPrep);
		TypeHandler<DateTime>::prepare(pos++, person.birthday, pPrep);
	}
};


} } // namespace Poco::Data


int main(int argc, char** argv)
{
	// register SQLite connector
	Poco::Data::SQLite::Connector::registerConnector();

	// create a session
	Session session("SQLite", "sample.db");

	// drop sample table, if it exists
	session << "DROP TABLE IF EXISTS Person", now;
	
	// (re)create table
	session << "CREATE TABLE Person (Name VARCHAR(30), Address VARCHAR, Age INTEGER(3), Birthday DATE)", now;
	
	// insert some rows
	Person person = 
	{
		"Bart Simpson",
		"Springfield",
		10,
		DateTime(1980, 4, 1)
	};
	
	Statement insert(session);
	insert << "INSERT INTO Person VALUES(?, ?, ?, ?)",
		use(person);
		
	insert.execute();
	
	person.name    = "Lisa Simpson";
	person.address = "Springfield";
	person.age     = 8;
	person.birthday = DateTime(1982, 5, 9);

	insert.execute();
	
	// a simple query
	Statement select(session);
	select << "SELECT Name, Address, Age, Birthday FROM Person",
		into(person),
		range(0, 1); //  iterate over result set one row at a time
		
	while (!select.done())
	{
		select.execute();
		std::cout << person.name << "\t"
			<< person.address << "\t"
			<< person.age << "\t"
			<< DateTimeFormatter::format(person.birthday, "%b %d %Y") 
		<< std::endl;
	}
	
	// another query - store the result in a container
	std::vector<Person> persons;
	session << "SELECT Name, Address, Age, Birthday FROM Person",
		into(persons),
		now;
		
	for (std::vector<Person>::const_iterator it = persons.begin(); it != persons.end(); ++it)
	{
		std::cout << it->name << "\t"
			<< it->address << "\t"
			<< it->age << "\t"
			<< DateTimeFormatter::format(it->birthday, "%b %d %Y") 
		<< std::endl;
	}
	
	return 0;
}
