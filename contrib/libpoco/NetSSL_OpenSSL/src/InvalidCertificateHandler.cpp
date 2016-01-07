//
// InvalidCertificateHandler.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/InvalidCertificateHandler.cpp#1 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  InvalidCertificateHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/InvalidCertificateHandler.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Delegate.h"


using Poco::Delegate;


namespace Poco {
namespace Net {


InvalidCertificateHandler::InvalidCertificateHandler(bool handleErrorsOnServerSide): _handleErrorsOnServerSide(handleErrorsOnServerSide)
{
	if (_handleErrorsOnServerSide)
		SSLManager::instance().ServerVerificationError += Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
	else
		SSLManager::instance().ClientVerificationError += Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
}


InvalidCertificateHandler::~InvalidCertificateHandler()
{
	try
	{
		if (_handleErrorsOnServerSide)
			SSLManager::instance().ServerVerificationError -= Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
		else
			SSLManager::instance().ClientVerificationError -= Delegate<InvalidCertificateHandler, VerificationErrorArgs>(this, &InvalidCertificateHandler::onInvalidCertificate);
	}
	catch (...)
	{
		poco_unexpected();
	}
}


} } // namespace Poco::Net
