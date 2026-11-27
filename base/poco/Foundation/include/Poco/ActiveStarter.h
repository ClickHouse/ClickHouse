//
// ActiveStarter.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the ActiveStarter class.
//
// Copyright (c) 2006-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ActiveStarter_INCLUDED
#define Foundation_ActiveStarter_INCLUDED


#include "Poco/ActiveRunnable.h"
#include "Poco/Foundation.h"


namespace Poco
{


/// The StarterType policy of ActiveMethod. Only specializations are defined (see ActiveDispatcher).
template <class OwnerType>
class ActiveStarter;


} // namespace Poco


#endif // Foundation_ActiveStarter_INCLUDED
