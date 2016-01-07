//
// NullPartHandler.cpp
//
// $Id: //poco/1.4/Net/src/NullPartHandler.cpp#1 $
//
// Library: Net
// Package: Messages
// Module:  NullPartHandler
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/NullPartHandler.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/NullStream.h"
#include "Poco/StreamCopier.h"


using Poco::NullOutputStream;
using Poco::StreamCopier;


namespace Poco {
namespace Net {


NullPartHandler::NullPartHandler()
{
}


NullPartHandler::~NullPartHandler()
{
}


void NullPartHandler::handlePart(const MessageHeader& header, std::istream& stream)
{
	NullOutputStream ostr;
	StreamCopier::copyStream(stream, ostr);
}


} } // namespace Poco::Net
