//
// OptionException.cpp
//
// Library: Util
// Package: Options
// Module:  OptionException
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/OptionException.h"
#include <typeinfo>


namespace Poco {
namespace Util {


POCO_IMPLEMENT_EXCEPTION(OptionException, Poco::DataException, "Option exception")
POCO_IMPLEMENT_EXCEPTION(UnknownOptionException, OptionException, "Unknown option specified")
POCO_IMPLEMENT_EXCEPTION(AmbiguousOptionException, OptionException, "Ambiguous option specified")
POCO_IMPLEMENT_EXCEPTION(MissingOptionException, OptionException, "Required option not specified")
POCO_IMPLEMENT_EXCEPTION(MissingArgumentException, OptionException, "Missing option argument")
POCO_IMPLEMENT_EXCEPTION(InvalidArgumentException, OptionException, "Invalid option argument")
POCO_IMPLEMENT_EXCEPTION(UnexpectedArgumentException, OptionException, "Unexpected option argument")
POCO_IMPLEMENT_EXCEPTION(IncompatibleOptionsException, OptionException, "Incompatible options")
POCO_IMPLEMENT_EXCEPTION(DuplicateOptionException, OptionException, "Option must not be given more than once")
POCO_IMPLEMENT_EXCEPTION(EmptyOptionException, OptionException, "Empty option specified")


} } // namespace Poco::Util
