//
// RecordSet.cpp
//
// $Id: //poco/Main/Data/samples/RecordSet/src/RecordSet.cpp#2 $
//
// This sample demonstrates the Data library.
//
/// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0


#include "Poco/SharedPtr.h"
#include "Poco/DateTime.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/Column.h"
#include "Poco/Data/SQLite/Connector.h"
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::DateTime;
using Poco::Data::Session;
using Poco::Data::Statement;
using Poco::Data::RecordSet;


int main(int argc, char** argv)
{
	// create a session
	Session session("SQLite", "sample.db");

	// drop sample table, if it exists
	session << "DROP TABLE IF EXISTS Person", now;
	
	// (re)create table
	session << "CREATE TABLE Person (Name VARCHAR(30), Address VARCHAR, Age INTEGER(3), Birthday DATE)", now;
	
	// insert some rows
	DateTime bd(1980, 4, 1);
	DateTime ld(1982, 5, 9);
	session << "INSERT INTO Person VALUES('Bart Simpson', 'Springfield', 12, ?)", use(bd), now;
	session << "INSERT INTO Person VALUES('Lisa Simpson', 'Springfield', 10, ?)", use(ld), now;
		
	// a simple query
	Statement select(session);
	select << "SELECT * FROM Person";
	select.execute();

	// create a RecordSet 
	RecordSet rs(select);
	std::size_t cols = rs.columnCount();
	// print all column names
	for (std::size_t col = 0; col < cols; ++col)
	{
		std::cout << rs.columnName(col) << std::endl;
	}
	// iterate over all rows and columns
	bool more = rs.moveFirst();
	while (more)
	{
		for (std::size_t col = 0; col < cols; ++col)
		{
			std::cout << rs[col].convert<std::string>() << " ";
		}
		std::cout << std::endl;
		more = rs.moveNext();
	}

	return 0;
}
