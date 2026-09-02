//
// PipeImpl.h
//
// Library: Foundation
// Package: Processes
// Module:  PipeImpl
//
// Definition of the PipeImpl class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PipeImpl_INCLUDED
#define Foundation_PipeImpl_INCLUDED


#include "Poco/Foundation.h"


#if   defined(POCO_OS_FAMILY_UNIX)
#    include "Poco/PipeImpl_POSIX.h"
#else
#    include "Poco/PipeImpl_DUMMY.h"
#endif


#endif // Foundation_PipeImpl_INCLUDED
