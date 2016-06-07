//
// OptionException.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/OptionException.h#1 $
//
// Library: Util
// Package: Options
// Module:  OptionException
//
// Definition of the OptionException class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_OptionException_INCLUDED
#define Util_OptionException_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Util {


POCO_DECLARE_EXCEPTION(Util_API, OptionException, Poco::DataException)
POCO_DECLARE_EXCEPTION(Util_API, UnknownOptionException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, AmbiguousOptionException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, MissingOptionException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, MissingArgumentException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, InvalidArgumentException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, UnexpectedArgumentException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, IncompatibleOptionsException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, DuplicateOptionException, OptionException)
POCO_DECLARE_EXCEPTION(Util_API, EmptyOptionException, OptionException)


} } // namespace Poco::Util


#endif // Util_OptionException_INCLUDED
