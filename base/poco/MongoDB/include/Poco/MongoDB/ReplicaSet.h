//
// ReplicaSet.h
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
	/// Class for working with a MongoDB replica set.
{
public:
	explicit ReplicaSet(const std::vector<Net::SocketAddress>& addresses);
		/// Creates the ReplicaSet using the given server addresses.

	virtual ~ReplicaSet();
		/// Destroys the ReplicaSet.

	Connection::Ptr findMaster();
		/// Tries to find the master MongoDB instance from the addresses
		/// passed to the constructor.
		///
		/// Returns the Connection to the master, or null if no master
		/// instance was found.

protected:
	Connection::Ptr isMaster(const Net::SocketAddress& host);

private:
	std::vector<Net::SocketAddress> _addresses;
};


} } // namespace Poco::MongoDB


#endif // MongoDB_ReplicaSet_INCLUDED
