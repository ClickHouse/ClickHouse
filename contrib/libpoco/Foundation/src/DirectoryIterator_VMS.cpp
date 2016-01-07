//
// DirectoryIterator_VMS.cpp
//
// $Id: //poco/1.4/Foundation/src/DirectoryIterator_VMS.cpp#1 $
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryIterator
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DirectoryIterator_VMS.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"
#include <iodef.h>
#include <atrdef.h>
#include <fibdef.h>
#include <starlet.h>


namespace Poco {


DirectoryIteratorImpl::DirectoryIteratorImpl(const std::string& path): _rc(1)
{
	Path p(path);
	p.makeDirectory();
	_search = p.toString();
	_search.append("*.*;*");

	_fab = cc$rms_fab;
	_fab.fab$l_fna = (char*) _search.c_str();
	_fab.fab$b_fns = _search.size();
	_fab.fab$l_nam = &_nam;

	_nam = cc$rms_nam;
	_nam.nam$l_esa = _spec;
	_nam.nam$b_ess = sizeof(_spec);

	if (sys$parse(&_fab) & 1)
		throw OpenFileException(path);

	next();
}


DirectoryIteratorImpl::~DirectoryIteratorImpl()
{
}


const std::string& DirectoryIteratorImpl::next()
{
	if (sys$search(&_fab) & 1)
		_current.clear();
	else
		_current.assign(_fab.fab$l_nam->nam$l_name, _fab.fab$l_nam->nam$b_name + _fab.fab$l_nam->nam$b_type);
	return _current;
}


} // namespace Poco
