//
// Tuple.cpp
//
// This sample demonstrates the Data library.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0


#include "Poco/SharedPtr.h"
#include "Poco/Tuple.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/SQLite/Connector.h"
#include <vector>
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;


int main(int argc, char** argv)
{
	typedef Poco::Tuple<std::string, std::string, int> Person;
	typedef std::vector<Person> People;

	// register SQLite connector
	Poco::Data::SQLite::Connector::registerConnector();

	// create a session
	Session session("SQLite", "sample.db");

	// drop sample table, if it exists
	session << "DROP TABLE IF EXISTS Person", now;
	
	// (re)create table
	session << "CREATE TABLE Person (Name VARCHAR(30), Address VARCHAR, Age INTEGER(3))", now;
	
	// insert some rows
	People people;
	people.push_back(Person("Bart Simpson",	"Springfield", 12));
	people.push_back(Person("Lisa Simpson",	"Springfield", 10));
	
	Statement insert(session);
	insert << "INSERT INTO Person VALUES(:name, :address, :age)",
		use(people), now;
	
	people.clear();

	// a simple query
	Statement select(session);
	select << "SELECT Name, Address, Age FROM Person",
		into(people),
		now;
	
	for (People::const_iterator it = people.begin(); it != people.end(); ++it)
	{
		std::cout << "Name: " << it->get<0>() << 
			", Address: " << it->get<1>() << 
			", Age: " << it->get<2>() <<std::endl;
	}

	return 0;
}
