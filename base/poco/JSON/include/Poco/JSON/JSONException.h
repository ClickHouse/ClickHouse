//
// JSONException.h
//
// Library: JSON
// Package: JSON
// Module:  JSONException
//
// Definition of the JSONException class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSONException_INCLUDED
#define JSON_JSONException_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/Exception.h"


namespace Poco {
namespace JSON {


POCO_DECLARE_EXCEPTION(JSON_API, JSONException, Poco::Exception)


} } // namespace Poco::JSON


#endif // JSON_JSONException_INCLUDED
