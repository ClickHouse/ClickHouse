//
// SSLException.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/SSLException.cpp#1 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  SSLException
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SSLException.h"
#include <typeinfo>


namespace Poco {
namespace Net {


POCO_IMPLEMENT_EXCEPTION(SSLException, NetException, "SSL Exception")
POCO_IMPLEMENT_EXCEPTION(SSLContextException, SSLException, "SSL context exception")
POCO_IMPLEMENT_EXCEPTION(InvalidCertificateException, SSLException, "Invalid certficate")
POCO_IMPLEMENT_EXCEPTION(CertificateValidationException, SSLException, "Certificate validation error")
POCO_IMPLEMENT_EXCEPTION(SSLConnectionUnexpectedlyClosedException, SSLException, "SSL connection unexpectedly closed")


} } // namespace Poco::Net
