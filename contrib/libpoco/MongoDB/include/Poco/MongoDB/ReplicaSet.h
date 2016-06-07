//
// ReplicaSet.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  ReplicaSet
//
// Definition of the ReplicaSet class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_ReplicaSet_INCLUDED
#define MongoDB_ReplicaSet_INCLUDED


#include "Poco/Net/SocketAddress.h"
#include "Poco/MongoDB/Connection.h"
#include <vector>


namespace Poco {
namespace MongoDB {


class MongoDB_API ReplicaSet
	/// Class for working with a replicaset
{
public:
	ReplicaSet(const std::vector<Net::SocketAddress>& addresses);
		/// Constructor

	virtual ~ReplicaSet();
		/// Destructor

	Connection::Ptr findMaster();
		/// Tries to find the master MongoDB instance from the addresses

private:
	Connection::Ptr isMaster(const Net::SocketAddress& host);

	std::vector<Net::SocketAddress> _addresses;
};


#endif // MongoDB_ReplicaSet_INCLUDED


} } // namespace Poco::MongoDB
