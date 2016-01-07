//
// Cursor.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Cursor
//
// Implementation of the Cursor class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Cursor.h"
#include "Poco/MongoDB/GetMoreRequest.h"
#include "Poco/MongoDB/KillCursorsRequest.h"


namespace Poco {
namespace MongoDB {


Cursor::Cursor(const std::string& db, const std::string& collection, QueryRequest::Flags flags)
	: _query(db + '.' + collection, flags)
{
}


Cursor::Cursor(const std::string& fullCollectionName, QueryRequest::Flags flags)
	: _query(fullCollectionName, flags)
{
}


Cursor::~Cursor()
{
	try
	{
		poco_assert_dbg(!_response.cursorID());

	}
	catch (...)
	{
	}
}


ResponseMessage& Cursor::next(Connection& connection)
{
	if ( _response.cursorID() == 0 )
	{
		connection.sendRequest(_query, _response);
	}
	else
	{
		Poco::MongoDB::GetMoreRequest getMore(_query.fullCollectionName(), _response.cursorID());
		getMore.setNumberToReturn(_query.getNumberToReturn());
		_response.clear();
		connection.sendRequest(getMore, _response);
	}
	return _response;
}


void Cursor::kill(Connection& connection)
{
	if ( _response.cursorID() != 0 )
	{
		KillCursorsRequest killRequest;
		killRequest.cursors().push_back(_response.cursorID());
		connection.sendRequest(killRequest);
	}
	_response.clear();
}


} } // Namespace Poco::MongoDB
