//
// NamedEvent_VMS.cpp
//
// $Id: //poco/1.4/Foundation/src/NamedEvent_VMS.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedEvent_VMS.h"
#include <lib$routines.h>
#include <starlet.h>
#include <descrip.h>
#include <iodef.h>


namespace Poco {


NamedEventImpl::NamedEventImpl(const std::string& name):
	_name(name)
{
	struct dsc$descriptor_s mbxDesc;
	mbxDesc.dsc$w_length  = _name.length();
	mbxDesc.dsc$b_dtype   = DSC$K_DTYPE_T;
	mbxDesc.dsc$b_class   = DSC$K_CLASS_S;
	mbxDesc.dsc$a_pointer = _name.c_str();
	if (sys$crembx(0, &_mbxChan, 0, 0, 0, 0, &mbxDesc, 0, 0) != 1)
		throw SystemException("cannot create named event", _name);
}


NamedEventImpl::~NamedEventImpl()
{
	sys$dassgn(_mbxChan);
}


void NamedEventImpl::setImpl()
{
	char buffer = 0xFF;
	if (sys$qio(0, _mbxChan, IO$_WRITEVBLK, 0, 0, 0, &buffer, sizeof(buffer), 0, 0, 0, 0) != 1)
		throw SystemException("cannot set named event", _name);
}


void NamedEventImpl::waitImpl()
{
	char buffer = 0;
	while (buffer == 0)
	{
		if (sys$qiow(0, _mbxChan, IO$_READVBLK, 0, 0, 0, &buffer, sizeof(buffer), 0, 0, 0, 0) != 1)
			throw SystemException("cannot wait for named event", _name);
	}
}


} // namespace Poco
