//
// main.cpp
//
// This sample shows SQL to mongo Shell to C++ examples.
//
// Copyright (c) 2013, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Connection.h"
#include "Poco/MongoDB/Database.h"
#include "Poco/MongoDB/Cursor.h"
#include "Poco/MongoDB/Array.h"


// INSERT INTO players
// VALUES( "Messi", "Lionel", 1987)
void sample1(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 1 ***" << std::endl;

	Poco::MongoDB::Database db("sample");
	Poco::SharedPtr<Poco::MongoDB::InsertRequest> insertPlayerRequest = db.createInsertRequest("players");

	// With one insert request, we can add multiple documents
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Valdes")
		.add("firstname", "Victor")
		.add("birthyear", 1982);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Alves")
		.add("firstname", "Daniel")
		.add("birthyear", 1983);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Bartra")
		.add("firstname", "Marc")
		.add("birthyear", 1991);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Alba")
		.add("firstname", "Jordi")
		.add("birthyear", 1989);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Montoya")
		.add("firstname", "Martin")
		.add("birthyear", 1991);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Abidal")
		.add("firstname", "Eric")
		.add("birthyear", 1979);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Fontas")
		.add("firstname", "Andreu")
		.add("birthyear", 1989);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Messi")
		.add("firstname", "Lionel")
		.add("birthyear", 1987);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Puyol")
		.add("firstname", "Carles")
		.add("birthyear", 1978);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Piqué")
		.add("firstname", "Gerard")
		.add("birthyear", 1987);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Muniesa")
		.add("firstname", "Marc")
		.add("birthyear", 1992);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Fabrégas")
		.add("firstname", "Cesc")
		.add("birthyear", 1987);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Hernandez")
		.add("firstname", "Xavi")
		.add("birthyear", 1980);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Iniesta")
		.add("firstname", "Andres")
		.add("birthyear", 1984);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Alcantara")
		.add("firstname", "Thiago")
		.add("birthyear", 1991);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Dos Santos")
		.add("firstname", "Jonathan")
		.add("birthyear", 1990);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Mascherano")
		.add("firstname", "Javier")
		.add("birthyear", 1984);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Busquets")
		.add("firstname", "Sergio")
		.add("birthyear", 1988);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Adriano")
		.add("firstname", "")
		.add("birthyear", 1984);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Song")
		.add("firstname", "Alex")
		.add("birthyear", 1987);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Villa")
		.add("firstname", "David")
		.add("birthyear", 1981);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Sanchez")
		.add("firstname", "Alexis")
		.add("birthyear", 1988);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Pedro")
		.add("firstname", "")
		.add("birthyear", 1987);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Cuenca")
		.add("firstname", "Isaac")
		.add("birthyear", 1991);
	insertPlayerRequest->addNewDocument()
		.add("lastname", "Tello")
		.add("firstname", "Cristian")
		.add("birthyear", 1991);

	std::cout << insertPlayerRequest->documents().size() << std::endl;

	connection.sendRequest(*insertPlayerRequest);
	std::string lastError = db.getLastError(connection);
	if (!lastError.empty())
	{
		std::cout << "Last Error: " << db.getLastError(connection) << std::endl;
	}
}


// SELECT lastname, birthyear FROM players
void sample2(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 2 ***" << std::endl;

	Poco::MongoDB::Cursor cursor("sample", "players");
	// Selecting fields is done by adding them to the returnFieldSelector
	// Use 1 as value of the element.
	cursor.query().returnFieldSelector().add("lastname", 1);
	cursor.query().returnFieldSelector().add("birthyear", 1);
	Poco::MongoDB::ResponseMessage& response = cursor.next(connection);
	for (;;)
	{
		for (Poco::MongoDB::Document::Vector::const_iterator it = response.documents().begin(); it != response.documents().end(); ++it)
		{
			std::cout << (*it)->get<std::string>("lastname") << " (" << (*it)->get<int>("birthyear") << ')' << std::endl;
		}

		// When the cursorID is 0, there are no documents left, so break out ...
		if (response.cursorID() == 0)
		{
			break;
		}

		// Get the next bunch of documents
		response = cursor.next(connection);
	}
}


// SELECT * FROM players
void sample3(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 3 ***" << std::endl;

	Poco::MongoDB::Cursor cursor("sample", "players");
	Poco::MongoDB::ResponseMessage& response = cursor.next(connection);
	for (;;)
	{
		for (Poco::MongoDB::Document::Vector::const_iterator it = response.documents().begin(); it != response.documents().end(); ++it)
		{
			std::cout << (*it)->get<std::string>("lastname") << ' ' << (*it)->get<std::string>("firstname") << " (" << (*it)->get<int>("birthyear") << ')' << std::endl;
		}

		// When the cursorID is 0, there are no documents left, so break out ...
		if (response.cursorID() == 0)
		{
			break;
		}

		// Get the next bunch of documents
		response = cursor.next(connection);
	};
}


// SELECT * FROM players WHERE birthyear = 1978
void sample4(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 4 ***" << std::endl;

	Poco::MongoDB::Cursor cursor("sample", "players");
	cursor.query().selector().add("birthyear", 1978);

	Poco::MongoDB::ResponseMessage& response = cursor.next(connection);
	for (;;)
	{
		for (Poco::MongoDB::Document::Vector::const_iterator it = response.documents().begin(); it != response.documents().end(); ++it)
		{
			std::cout << (*it)->get<std::string>("lastname") << ' ' << (*it)->get<std::string>("firstname") << " (" << (*it)->get<int>("birthyear") << ')' << std::endl;
		}

		// When the cursorID is 0, there are no documents left, so break out ...
		if (response.cursorID() == 0)
		{
			break;
		}

		// Get the next bunch of documents
		response = cursor.next(connection);
	};
}


// SELECT * FROM players WHERE birthyear = 1987 ORDER BY name
void sample5(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 5 ***" << std::endl;

	Poco::MongoDB::Cursor cursor("sample", "players");

	// When orderby is needed, use 2 separate documents in the query selector
	cursor.query().selector().addNewDocument("$query").add("birthyear", 1987);
	cursor.query().selector().addNewDocument("$orderby").add("lastname", 1);

	Poco::MongoDB::ResponseMessage& response = cursor.next(connection);
	for (;;)
	{
		for (Poco::MongoDB::Document::Vector::const_iterator it = response.documents().begin(); it != response.documents().end(); ++it)
		{
			std::cout << (*it)->get<std::string>("lastname") << ' ' << (*it)->get<std::string>("firstname") << " (" << (*it)->get<int>("birthyear") << ')' << std::endl;
		}

		// When the cursorID is 0, there are no documents left, so break out ...
		if (response.cursorID() == 0)
		{
			break;
		}

		// Get the next bunch of documents
		response = cursor.next(connection);
	};
}


// SELECT * FROM players WHERE birthyear > 1969 and birthyear <= 1980
void sample6(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 6 ***" << std::endl;

	Poco::MongoDB::Cursor cursor("sample", "players");

	cursor.query().selector().addNewDocument("birthyear")
		.add("$gt", 1969)
		.add("$lte", 1980);

	Poco::MongoDB::ResponseMessage& response = cursor.next(connection);
	for (;;)
	{
		for (Poco::MongoDB::Document::Vector::const_iterator it = response.documents().begin(); it != response.documents().end(); ++it)
		{
			std::cout << (*it)->get<std::string>("lastname") << ' ' << (*it)->get<std::string>("firstname") << " (" << (*it)->get<int>("birthyear") << ')' << std::endl;
		}

		// When the cursorID is 0, there are no documents left, so break out ...
		if (response.cursorID() == 0)
		{
			break;
		}

		// Get the next bunch of documents
		response = cursor.next(connection);
	};
}


// CREATE INDEX playername
// ON players(lastname)
void sample7(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 7 ***" << std::endl;

	Poco::MongoDB::Database db("sample");
	Poco::MongoDB::Document::Ptr keys = new Poco::MongoDB::Document();
	keys->add("lastname", 1);
	Poco::MongoDB::Document::Ptr errorDoc = db.ensureIndex(connection, "players", "lastname", keys);

	/* Sample above is the same as the following code:
	Poco::MongoDB::Document::Ptr index = new Poco::MongoDB::Document();
	index->add("ns", "sample.players");
	index->add("name", "lastname");
	index->addNewDocument("key").add("lastname", 1);

	Poco::SharedPtr<Poco::MongoDB::InsertRequest> insertRequest = db.createInsertRequest("system.indexes");
	insertRequest->documents().push_back(index);
	connection.sendRequest(*insertRequest);
	Poco::MongoDB::Document::Ptr errorDoc = db.getLastErrorDoc(connection);
	*/
	std::cout << errorDoc->toString(2);
}


// SELECT * FROM players LIMIT 10 SKIP 20
void sample8(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 8 ***" << std::endl;

	Poco::MongoDB::Cursor cursor("sample", "players");
	cursor.query().setNumberToReturn(10);
	cursor.query().setNumberToSkip(20);
	Poco::MongoDB::ResponseMessage& response = cursor.next(connection);
	for (;;)
	{
		for (Poco::MongoDB::Document::Vector::const_iterator it = response.documents().begin(); it != response.documents().end(); ++it)
		{
			std::cout << (*it)->get<std::string>("lastname") << ' ' << (*it)->get<std::string>("firstname") << " (" << (*it)->get<int>("birthyear") << ')' << std::endl;
		}

		// When the cursorID is 0, there are no documents left, so break out ...
		if (response.cursorID() == 0)
		{
			break;
		}

		// Get the next bunch of documents
		response = cursor.next(connection);
	};
}

// SELECT * FROM players LIMIT 1
void sample9(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 9 ***" << std::endl;

	// QueryRequest can be used directly
	Poco::MongoDB::QueryRequest query("sample.players");
	query.setNumberToReturn(1);
	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(query, response);
	if (response.hasDocuments())
	{
		std::cout << response.documents()[0]->toString(2) << std::endl;
	}

	// QueryRequest can be created using the Database class
	Poco::MongoDB::Database db("sample");
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> queryPtr = db.createQueryRequest("players");
	queryPtr->setNumberToReturn(1);
	connection.sendRequest(*queryPtr, response);
	if (response.hasDocuments())
	{
		std::cout << response.documents()[0]->toString(2) << std::endl;
	}
}

// SELECT DISTINCT birthyear FROM players WHERE birthyear > 1980
void sample10(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 10 ***" << std::endl;

	Poco::MongoDB::Database db("sample");
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> command = db.createCommand();

	command->selector()
		.add("distinct", "players")
		.add("key", "birthyear")
		.addNewDocument("query")
		.addNewDocument("birthyear")
		.add("$gt", 1980);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*command, response);
	if (response.hasDocuments())
	{
		Poco::MongoDB::Array::Ptr values = response.documents()[0]->get<Poco::MongoDB::Array::Ptr>("values");
		for (int i = 0; i < values->size(); ++i )
		{
			std::cout << values->get<int>(i) << std::endl;
		}
	}

}

// SELECT COUNT(*) FROM players WHERE birthyear > 1980
void sample11(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 11 ***" << std::endl;

	Poco::MongoDB::Database db("sample");
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> count = db.createCountRequest("players");
	count->selector().addNewDocument("query")
		.addNewDocument("birthyear")
			.add("$gt", 1980);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*count, response);

	if (response.hasDocuments())
	{
		std::cout << "Count: " << response.documents()[0]->getInteger("n") << std::endl;
	}
}


//UPDATE players SET birthyear = birthyear + 1 WHERE firstname = 'Victor'
void sample12(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 12 ***" << std::endl;

	Poco::MongoDB::Database db("sample");
	Poco::SharedPtr<Poco::MongoDB::UpdateRequest> request = db.createUpdateRequest("players");
	request->selector().add("firstname", "Victor");

	request->update().addNewDocument("$inc").add("birthyear", 1);

	connection.sendRequest(*request);

	Poco::MongoDB::Document::Ptr lastError = db.getLastErrorDoc(connection);
	std::cout << "LastError: " << lastError->toString(2) << std::endl;
}


//DELETE players WHERE firstname = 'Victor'
void sample13(Poco::MongoDB::Connection& connection)
{
	std::cout << "*** SAMPLE 13 ***" << std::endl;

	Poco::MongoDB::Database db("sample");
	Poco::SharedPtr<Poco::MongoDB::DeleteRequest> request = db.createDeleteRequest("players");
	request->selector().add("firstname", "Victor");

	connection.sendRequest(*request);

	Poco::MongoDB::Document::Ptr lastError = db.getLastErrorDoc(connection);
	std::cout << "LastError: " << lastError->toString(2) << std::endl;
}


int main(int argc, char** argv)
{
	Poco::MongoDB::Connection connection("localhost", 27017);

	try
	{
		sample1(connection);
		sample2(connection);
		sample3(connection);
		sample4(connection);
		sample5(connection);
		sample6(connection);
		sample7(connection);
		sample8(connection);
		sample9(connection);
		sample10(connection);
		sample11(connection);
		sample12(connection);
		sample13(connection);
	}
	catch (Poco::Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
	}

	return 0;
}
