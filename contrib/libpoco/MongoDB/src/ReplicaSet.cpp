//
// ReplicaSet.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  ReplicaSet
//
// Implementation of the ReplicaSet class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/ReplicaSet.h"
#include "Poco/MongoDB/QueryRequest.h"
#include "Poco/MongoDB/ResponseMessage.h"

namespace Poco {
namespace MongoDB {


ReplicaSet::ReplicaSet(const std::vector<Net::SocketAddress> &addresses) : _addresses(addresses)
{
}


ReplicaSet::~ReplicaSet()
{
}


Connection::Ptr ReplicaSet::findMaster()
{
	Connection::Ptr master;

	for(std::vector<Net::SocketAddress>::iterator it = _addresses.begin(); it != _addresses.end(); ++it)
	{
		master = isMaster(*it);
		if ( ! master.isNull() )
		{
			break;
		}
	}

	return master;
}


Connection::Ptr ReplicaSet::isMaster(const Net::SocketAddress& address)
{
	Connection::Ptr conn = new Connection();

	try
	{
		conn->connect(address);

		QueryRequest request("admin.$cmd");
		request.setNumberToReturn(1);
		request.selector().add("isMaster", 1);

		ResponseMessage response;
		conn->sendRequest(request, response);

		if ( response.documents().size() > 0 )
		{
			Document::Ptr doc = response.documents()[0];
			if ( doc->get<bool>("ismaster") )
			{
				return conn;
			}
			else if ( doc->exists("primary") )
			{
				return isMaster(Net::SocketAddress(doc->get<std::string>("primary")));
			}
		}
	}
	catch(...)
	{
		conn = NULL;
	}
	
	return NULL; 
}


} } // namespace Poco::MongoDB
