//
// RowFormatter.cpp
//
// $Id: //poco/Main/Data/samples/RecordSet/src/RowFormatter.cpp#2 $
//
// This sample demonstrates the Data library recordset row formatting
// and streaming capabilities.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0


#include "Poco/SharedPtr.h"
#include "Poco/DateTime.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/Statement.h"
#include "Poco/Data/RecordSet.h"
#include "Poco/Data/RowFormatter.h"
#include "Poco/Data/SQLite/Connector.h"
#include <iostream>


using namespace Poco::Data::Keywords;
using Poco::DateTime;
using Poco::Data::Session;
using Poco::Data::Statement;
using Poco::Data::RecordSet;
using Poco::Data::RowFormatter;


class HTMLTableFormatter : public RowFormatter
{
public:
	HTMLTableFormatter()
	{
		std::ostringstream os;
		os << "<TABLE border=\"1\" cellspacing=\"0\">" << std::endl;
		setPrefix(os.str());
		
		os.str("");
		os << "</TABLE>" << std::endl;
		setPostfix(os.str());
	}

	std::string& formatNames(const NameVecPtr pNames, std::string& formattedNames)
	{
		std::ostringstream str;

		str << "\t<TR>" << std::endl;
		NameVec::const_iterator it = pNames->begin();
		NameVec::const_iterator end = pNames->end();
		for (; it != end; ++it)	str << "\t\t<TH align=\"center\">" << *it << "</TH>" << std::endl;
		str << "\t</TR>" << std::endl;

		return formattedNames = str.str();
	}

	std::string& formatValues(const ValueVec& vals, std::string& formattedValues)
	{
		std::ostringstream str;

		str << "\t<TR>" << std::endl;
		ValueVec::const_iterator it = vals.begin();
		ValueVec::const_iterator end = vals.end();
		for (; it != end; ++it)
		{
			if (it->isNumeric()) 
				str << "\t\t<TD align=\"right\">";
			else 
				str << "\t\t<TD align=\"left\">";

			str << it->convert<std::string>() << "</TD>" << std::endl;
		}
		str << "\t</TR>" << std::endl;

		return formattedValues = str.str();
	}
};


int main(int argc, char** argv)
{
	// register SQLite connector
	Poco::Data::SQLite::Connector::registerConnector();
	
	// create a session
	Session session("SQLite", "sample.db");

	// drop sample table, if it exists
	session << "DROP TABLE IF EXISTS Simpsons", now;
	
	// (re)create table
	session << "CREATE TABLE Simpsons (Name VARCHAR(30), Address VARCHAR, Age INTEGER(3), Birthday DATE)", now;
	
	// insert some rows
	DateTime hd(1956, 3, 1);
	session << "INSERT INTO Simpsons VALUES('Homer Simpson', 'Springfield', 42, ?)", use(hd), now;
	hd.assign(1954, 10, 1);
	session << "INSERT INTO Simpsons VALUES('Marge Simpson', 'Springfield', 38, ?)", use(hd), now;
	hd.assign(1980, 4, 1);
	session << "INSERT INTO Simpsons VALUES('Bart Simpson', 'Springfield', 12, ?)", use(hd), now;
	hd.assign(1982, 5, 9);
	session << "INSERT INTO Simpsons VALUES('Lisa Simpson', 'Springfield', 10, ?)", use(hd), now;
		
	// create a statement and print the column names and data as HTML table
	HTMLTableFormatter tf;
	Statement stmt = (session << "SELECT * FROM Simpsons", format(tf), now);
	RecordSet rs(stmt);
	std::cout << rs << std::endl;

	// Note: The code above is divided into individual steps for clarity purpose.
	// The four lines can be reduced to the following single line:
	std::cout << RecordSet(session, "SELECT * FROM Simpsons", HTMLTableFormatter());

	// simple formatting example (uses the default SimpleRowFormatter provided by framework)
	std::cout << std::endl << "Simple formatting:" << std::endl << std::endl;
	std::cout << RecordSet(session, "SELECT * FROM Simpsons");

	return 0;
}
