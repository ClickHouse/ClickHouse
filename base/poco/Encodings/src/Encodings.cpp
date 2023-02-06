//
// Encodings.cpp
//
// Library: Encodings
// Package: Encodings
// Module:  Encodings
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Encodings.h"
#include "Poco/TextEncoding.h"
#include "Poco/ISO8859_10Encoding.h"
#include "Poco/ISO8859_11Encoding.h"
#include "Poco/ISO8859_13Encoding.h"
#include "Poco/ISO8859_14Encoding.h"
#include "Poco/ISO8859_16Encoding.h"
#include "Poco/ISO8859_3Encoding.h"
#include "Poco/ISO8859_4Encoding.h"
#include "Poco/ISO8859_5Encoding.h"
#include "Poco/ISO8859_6Encoding.h"
#include "Poco/ISO8859_7Encoding.h"
#include "Poco/ISO8859_8Encoding.h"
#include "Poco/ISO8859_9Encoding.h"
#include "Poco/Windows1253Encoding.h"
#include "Poco/Windows1254Encoding.h"
#include "Poco/Windows1255Encoding.h"
#include "Poco/Windows1256Encoding.h"
#include "Poco/Windows1257Encoding.h"
#include "Poco/Windows1258Encoding.h"
#include "Poco/Windows874Encoding.h"
#include "Poco/Windows932Encoding.h"
#include "Poco/Windows936Encoding.h"
#include "Poco/Windows949Encoding.h"
#include "Poco/Windows950Encoding.h"


namespace Poco {


void registerExtraEncodings()
{
	TextEncoding::add(new ISO8859_10Encoding);
	TextEncoding::add(new ISO8859_11Encoding);
	TextEncoding::add(new ISO8859_13Encoding);
	TextEncoding::add(new ISO8859_14Encoding);
	TextEncoding::add(new ISO8859_16Encoding);
	TextEncoding::add(new ISO8859_3Encoding);
	TextEncoding::add(new ISO8859_4Encoding);
	TextEncoding::add(new ISO8859_5Encoding);
	TextEncoding::add(new ISO8859_6Encoding);
	TextEncoding::add(new ISO8859_7Encoding);
	TextEncoding::add(new ISO8859_8Encoding);
	TextEncoding::add(new ISO8859_9Encoding);
	TextEncoding::add(new Windows1253Encoding);
	TextEncoding::add(new Windows1254Encoding);
	TextEncoding::add(new Windows1255Encoding);
	TextEncoding::add(new Windows1256Encoding);
	TextEncoding::add(new Windows1257Encoding);
	TextEncoding::add(new Windows1258Encoding);
	TextEncoding::add(new Windows874Encoding);
	TextEncoding::add(new Windows932Encoding);
	TextEncoding::add(new Windows936Encoding);
	TextEncoding::add(new Windows949Encoding);
	TextEncoding::add(new Windows950Encoding);
}


} // namespace Poco

