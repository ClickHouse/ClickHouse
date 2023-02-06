//
// ApacheChannel.h
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ApacheConnector_ApacheChannel_INCLUDED
#define ApacheConnector_ApacheChannel_INCLUDED


#include "Poco/Channel.h"


class ApacheChannel: public Poco::Channel
	/// This class implements a logging channel
	/// that uses the Apache logging facilities.
{
public:
	ApacheChannel();
	~ApacheChannel();

	void log(const Poco::Message& msg);
};


#endif // ApacheConnector_ApacheChannel_INCLUDED
