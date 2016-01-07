//
// NamedMutex_VMS.cpp
//
// $Id: //poco/1.4/Foundation/src/NamedMutex_VMS.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedMutex_VMS.h"
#include <starlet.h>
#include <iodef.h>


namespace Poco {


NamedMutexImpl::NamedMutexImpl(const std::string& name):
	_name(name)
{
	_nameDesc.dsc$w_length  = _name.length();
	_nameDesc.dsc$b_dtype   = DSC$K_DTYPE_T;
	_nameDesc.dsc$b_class   = DSC$K_CLASS_S;
	_nameDesc.dsc$a_pointer = _name.data();
	int status = sys$enqw(0, LCK$K_NLMODE, (struct _lksb*) &_lksb, 0, &_nameDesc, 0, 0, 0, 0, 0, 0);
	if (status != 1)
		throw SystemException("cannot create named mutex", _name);
}


NamedMutexImpl::~NamedMutexImpl()
{
	sys$deq(m_lksb[1], 0, 0, 0);
}


void NamedMutexImpl::lockImpl()
{
	int status = sys$enqw(0, LCK$K_EXMODE, (struct _lksb*) &_lksb, LCK$M_CONVERT, &_nameDesc, 0, 0, 0, 0, 0, 0);
	if (status != 1)
		throw SystemException("cannot lock named mutex", _name);
}


bool NamedMutexImpl::tryLockImpl()
{
	int status = sys$enqw(0, LCK$K_EXMODE, (struct _lksb*) &_lksb, LCK$M_CONVERT | LCK$M_NOQUEUE, &_nameDesc, 0, 0, 0, 0, 0, 0);
	return status == 1;
}


void NamedMutexImpl::unlockImpl()
{
	int status = sys$enqw(0, LCK$K_NLMODE, (struct _lksb*) &_lksb, LCK$M_CONVERT, &_nameDesc, 0, 0, 0, 0, 0, 0);
	if (status != 1)
		throw SystemException("cannot unlock named mutex", _name);
}


} // namespace Poco
