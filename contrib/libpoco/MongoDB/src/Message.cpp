//
// Message.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Message
//
// Implementation of the Message class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Message.h"


namespace Poco {
namespace MongoDB {


Message::Message(MessageHeader::OpCode opcode) : _header(opcode)
{
}


Message::~Message()
{
}


} } // namespace Poco::MongoDB
