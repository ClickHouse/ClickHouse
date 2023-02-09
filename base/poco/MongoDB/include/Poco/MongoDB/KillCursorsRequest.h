//
// KillCursorsRequest.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  KillCursorsRequest
//
// Definition of the KillCursorsRequest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_KillCursorsRequest_INCLUDED
#define MongoDB_KillCursorsRequest_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/RequestMessage.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API KillCursorsRequest: public RequestMessage
	/// Class for creating an OP_KILL_CURSORS client request. This
	/// request is used to kill cursors, which are still open, 
	/// returned by query requests.
{
public:
	KillCursorsRequest();
		/// Creates a KillCursorsRequest.

	virtual ~KillCursorsRequest();
		/// Destroys the KillCursorsRequest.
		
	std::vector<Int64>& cursors();
		/// The internal list of cursors.
		
protected:
	void buildRequest(BinaryWriter& writer);
	std::vector<Int64> _cursors;
};


//
// inlines
//
inline std::vector<Int64>& KillCursorsRequest::cursors()
{
	return _cursors;
} 


} } // namespace Poco::MongoDB


#endif // MongoDB_KillCursorsRequest_INCLUDED
