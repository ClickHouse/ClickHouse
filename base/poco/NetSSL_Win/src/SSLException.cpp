//
// SSLException.cpp
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  SSLException
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
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
POCO_IMPLEMENT_EXCEPTION(CertificateException, SSLException, "Certificate exception")
POCO_IMPLEMENT_EXCEPTION(NoCertificateException, CertificateException, "No certificate")
POCO_IMPLEMENT_EXCEPTION(InvalidCertificateException, CertificateException, "Invalid certficate")
POCO_IMPLEMENT_EXCEPTION(CertificateValidationException, CertificateException, "Certificate validation error")
POCO_IMPLEMENT_EXCEPTION(SSLConnectionUnexpectedlyClosedException, SSLException, "SSL connection unexpectedly closed")


} } // namespace Poco::Net
