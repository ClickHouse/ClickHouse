//
// OpMsgCursor.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  OpMsgCursor
//
// Copyright (c) 2022, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/OpMsgCursor.h"
#include "Poco/MongoDB/Array.h"

//
// NOTE:
//
// MongoDB specification indicates that the flag MSG_EXHAUST_ALLOWED shall be
// used in the request when the receiver is ready to receive multiple messages
// without sending additional requests in between. Sender (MongoDB) indicates
// that more messages follow with flag MSG_MORE_TO_COME.
//
// It seems that this does not work properly. MSG_MORE_TO_COME is set and reading
// next messages sometimes works, however often the data is missing in response
// or the message header contains wrong message length and reading blocks.
// Opcode in the header is correct.
//
// Using MSG_EXHAUST_ALLOWED is therefore currently disabled.
//
// It seems that related JIRA ticket is:
//
// https://jira.mongodb.org/browse/SERVER-57297
//
// https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst
//

#define MONGODB_EXHAUST_ALLOWED_WORKS	false

namespace Poco {
namespace MongoDB {


[[ maybe_unused ]] static const std::string keyCursor		{"cursor"};
[[ maybe_unused ]] static const std::string keyFirstBatch	{"firstBatch"};
[[ maybe_unused ]] static const std::string keyNextBatch	{"nextBatch"};

static Poco::Int64 cursorIdFromResponse(const MongoDB::Document& doc);


OpMsgCursor::OpMsgCursor(const std::string& db, const std::string& collection):
#if MONGODB_EXHAUST_ALLOWED_WORKS
	_query(db, collection, OpMsgMessage::MSG_EXHAUST_ALLOWED)
#else
	_query(db, collection)
#endif
{
}

OpMsgCursor::~OpMsgCursor()
{
	try
	{
		poco_assert_dbg(_cursorID == 0);
	}
	catch (...)
	{
	}
}


void OpMsgCursor::setEmptyFirstBatch(bool empty)
{
	_emptyFirstBatch = empty;
}


bool OpMsgCursor::emptyFirstBatch() const
{
	return _emptyFirstBatch;
}


void OpMsgCursor::setBatchSize(Int32 batchSize)
{
	_batchSize = batchSize;
}


Int32 OpMsgCursor::batchSize() const
{
	return _batchSize;
}


OpMsgMessage& OpMsgCursor::next(Connection& connection)
{
	if (_cursorID == 0)
	{
		_response.clear();

		if (_emptyFirstBatch || _batchSize > 0)
		{
			Int32 bsize = _emptyFirstBatch ? 0 : _batchSize;
			if (_query.commandName() == OpMsgMessage::CMD_FIND)
			{
				_query.body().add("batchSize", bsize);
			}
			else if (_query.commandName() == OpMsgMessage::CMD_AGGREGATE)
			{
				auto& cursorDoc = _query.body().addNewDocument("cursor");
				cursorDoc.add("batchSize", bsize);
			}
		}

		connection.sendRequest(_query, _response);

		const auto& rdoc = _response.body();
		_cursorID = cursorIdFromResponse(rdoc);
	}
	else
	{
#if MONGODB_EXHAUST_ALLOWED_WORKS
		std::cout << "Response flags: " << _response.flags() << std::endl;
		if (_response.flags() & OpMsgMessage::MSG_MORE_TO_COME)
		{
			std::cout << "More to come. Reading more response: " << std::endl;
			_response.clear();
			connection.readResponse(_response);
		}
		else
#endif
		{
			_response.clear();
			_query.setCursor(_cursorID, _batchSize);
			connection.sendRequest(_query, _response);
		}
	}

	const auto& rdoc = _response.body();
	_cursorID = cursorIdFromResponse(rdoc);

	return _response;
}


void OpMsgCursor::kill(Connection& connection)
{
	_response.clear();
	if (_cursorID != 0)
	{
		_query.setCommandName(OpMsgMessage::CMD_KILL_CURSORS);

		MongoDB::Array::Ptr cursors = new MongoDB::Array();
		cursors->add<Poco::Int64>(_cursorID);
		_query.body().add("cursors", cursors);

		connection.sendRequest(_query, _response);

		const auto killed = _response.body().get<MongoDB::Array::Ptr>("cursorsKilled", nullptr);
		if (!killed || killed->size() != 1 || killed->get<Poco::Int64>(0, -1) != _cursorID)
		{
			throw Poco::ProtocolException("Cursor not killed as expected: " + std::to_string(_cursorID));
		}

		_cursorID = 0;
		_query.clear();
		_response.clear();
	}
}


Poco::Int64 cursorIdFromResponse(const MongoDB::Document& doc)
{
	Poco::Int64 id {0};
	auto cursorDoc = doc.get<Document::Ptr>(keyCursor, nullptr);
	if(cursorDoc)
	{
		id = cursorDoc->get<Poco::Int64>("id", 0);
	}
	return id;
}


} } // Namespace Poco::MongoDB
