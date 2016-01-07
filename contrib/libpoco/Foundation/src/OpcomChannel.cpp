//
// OpcomChannel.cpp
//
// $Id: //poco/1.4/Foundation/src/OpcomChannel.cpp#1 $
//
// Library: Foundation
// Package: Logging
// Module:  OpcomChannel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/OpcomChannel.h"
#include "Poco/Message.h"
#include <starlet.h>
#include <opcdef.h>
#include <descrip.h>


namespace Poco {


const std::string OpcomChannel::PROP_TARGET = "target";


OpcomChannel::OpcomChannel(): _target(OPC$M_NM_CENTRL)
{
}


OpcomChannel::OpcomChannel(int target): _target(target)
{
}


OpcomChannel::~OpcomChannel()
{
}


void OpcomChannel::log(const Message& msg)
{
	const std::string& text = msg.getText();
	// set up OPC buffer
	struct _opcdef buffer;
	buffer.opc$b_ms_type   = OPC$_RQ_RQST;
	buffer.opc$b_ms_target = _target;
	buffer.opc$l_ms_rqstid = 0;
	int len = text.size();
	// restrict message text to 128 chars
	if (len > 128) len = 128;
	// copy message text into buffer
	memcpy(&buffer.opc$l_ms_text, text.data(), len);

	// sys$sndopr only accepts 32-bit pointers
	#pragma pointer_size save
	#pragma pointer_size 32

	// set up the descriptor
	struct dsc$descriptor bufferDsc;
	bufferDsc.dsc$w_length  = len + 8;
	bufferDsc.dsc$a_pointer = (char*) &buffer;
	// call the system service
	sys$sndopr(&bufferDsc, 0);

	#pragma pointer_size restore
}


void OpcomChannel::setProperty(const std::string& name, const std::string& value)
{
	if (name == PROP_TARGET)
	{
		if (value == "CARDS")
			_target = OPC$M_NM_CARDS;
		else if (value == "CENTRL")
			_target = OPC$M_NM_CENTRL;
		else if (value == "CLUSTER")
			_target = OPC$M_NM_CLUSTER;
		else if (value == "DEVICE")
			_target = OPC$M_NM_DEVICE;
		else if (value == "DISKS")
			_target = OPC$M_NM_DISKS;
		else if (value == "NTWORK")
			_target = OPC$M_NM_NTWORK;
		else if (value == "TAPES")
			_target = OPC$M_NM_TAPES;
		else if (value == "PRINT")
			_target = OPC$M_NM_PRINT;
		else if (value == "SECURITY")
			_target = OPC$M_NM_SECURITY;
		else if (value == "OPER1")
			_target = OPC$M_NM_OPER1;
		else if (value == "OPER2")
			_target = OPC$M_NM_OPER2;
		else if (value == "OPER3")
			_target = OPC$M_NM_OPER3;
		else if (value == "OPER4")
			_target = OPC$M_NM_OPER4;
		else if (value == "OPER5")
			_target = OPC$M_NM_OPER5;
		else if (value == "OPER6")
			_target = OPC$M_NM_OPER6;
		else if (value == "OPER7")
			_target = OPC$M_NM_OPER7;
		else if (value == "OPER8")
			_target = OPC$M_NM_OPER8;
		else if (value == "OPER9")
			_target = OPC$M_NM_OPER9;
		else if (value == "OPER10")
			_target = OPC$M_NM_OPER10;
		else if (value == "OPER11")
			_target = OPC$M_NM_OPER11;
		else if (value == "OPER12")
			_target = OPC$M_NM_OPER12;
	}
	else
	{
		Channel::setProperty(name, value);
	}
}


std::string OpcomChannel::getProperty(const std::string& name) const
{
	if (name == PROP_TARGET)
	{
		if (_target == OPC$M_NM_CARDS)
			return "CARDS";
		else if (_target == OPC$M_NM_CENTRL)
			return "CENTRL";
		else if (_target == OPC$M_NM_CLUSTER)
			return "CLUSTER";
		else if (_target == OPC$M_NM_DEVICE)
			return "DEVICE";
		else if (_target == OPC$M_NM_DISKS)
			return "DISKS";
		else if (_target == OPC$M_NM_NTWORK)
			return "NTWORK";
		else if (_target == OPC$M_NM_TAPES)
			return "TAPES";
		else if (_target == OPC$M_NM_PRINT)
			return "PRINT";
		else if (_target == OPC$M_NM_SECURITY)
			return "SECURITY";
		else if (_target == OPC$M_NM_OPER1)
			return "OPER1";
		else if (_target == OPC$M_NM_OPER2)
			return "OPER2";
		else if (_target == OPC$M_NM_OPER3)
			return "OPER3";
		else if (_target == OPC$M_NM_OPER4)
			return "OPER4";
		else if (_target == OPC$M_NM_OPER5)
			return "OPER5";
		else if (_target == OPC$M_NM_OPER6)
			return "OPER6";
		else if (_target == OPC$M_NM_OPER7)
			return "OPER7";
		else if (_target == OPC$M_NM_OPER8)
			return "OPER8";
		else if (_target == OPC$M_NM_OPER9)
			return "OPER9";
		else if (_target == OPC$M_NM_OPER10)
			return "OPER10";
		else if (_target == OPC$M_NM_OPER11)
			return "OPER11";
		else if (_target == OPC$M_NM_OPER12)
			return "OPER12";
	}
	return Channel::getProperty(name);
}


} // namespace Poco
