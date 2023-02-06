//
// ApacheApplication.cpp
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ApacheChannel.h"
#include "ApacheConnector.h"
#include "Poco/Message.h"


ApacheChannel::ApacheChannel()
{
}


ApacheChannel::~ApacheChannel()
{
}


void ApacheChannel::log(const Poco::Message& msg)
{
	ApacheConnector::log(msg.getSource().c_str(), 0, msg.getPriority(), 0, msg.getText().c_str());
}
