//
// DirectoryIterator_VMS.h
//
// $Id: //poco/1.4/Foundation/include/Poco/DirectoryIterator_VMS.h#1 $
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryIterator
//
// Definition of the DirectoryIteratorImpl class for OpenVMS.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DirectoryIterator_VMS_INCLUDED
#define Foundation_DirectoryIterator_VMS_INCLUDED


#include "Poco/Foundation.h"
#include <rms.h>


namespace Poco {


class Foundation_API DirectoryIteratorImpl
{
public:
	DirectoryIteratorImpl(const std::string& path);
	~DirectoryIteratorImpl();
	
	void duplicate();
	void release();
	
	const std::string& get() const;
	const std::string& next();
	
private:
	struct FAB  _fab;
	struct NAM  _nam;
	std::string _search;
	char        _spec[255];
	std::string _current;
	int _rc;
};


//
// inlines
//
const std::string& DirectoryIteratorImpl::get() const
{
	return _current;
}


inline void DirectoryIteratorImpl::duplicate()
{
	++_rc;
}


inline void DirectoryIteratorImpl::release()
{
	if (--_rc == 0)
		delete this;
}


} // namespace Poco


#endif // Foundation_DirectoryIterator_VMS_INCLUDED
