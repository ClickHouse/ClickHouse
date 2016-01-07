//
// JSONException.cpp
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  JSONException
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/JSONException.h"
#include <typeinfo>


namespace Poco {
namespace JSON {


POCO_IMPLEMENT_EXCEPTION(JSONException, Exception, "JSON Exception")


} } // Namespace Poco::JSON
