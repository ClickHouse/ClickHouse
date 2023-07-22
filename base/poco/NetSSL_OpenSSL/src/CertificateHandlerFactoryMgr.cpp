//
// CertificateHandlerFactoryMgr.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  CertificateHandlerFactoryMgr
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/CertificateHandlerFactoryMgr.h"
#include "Poco/Net/ConsoleCertificateHandler.h"
#include "Poco/Net/AcceptCertificateHandler.h"
#include "Poco/Net/RejectCertificateHandler.h"


namespace Poco {
namespace Net {


CertificateHandlerFactoryMgr::CertificateHandlerFactoryMgr()
{
	setFactory("ConsoleCertificateHandler", new CertificateHandlerFactoryImpl<ConsoleCertificateHandler>());
	setFactory("AcceptCertificateHandler", new CertificateHandlerFactoryImpl<AcceptCertificateHandler>());
	setFactory("RejectCertificateHandler", new CertificateHandlerFactoryImpl<RejectCertificateHandler>());
}


CertificateHandlerFactoryMgr::~CertificateHandlerFactoryMgr()
{
}


void CertificateHandlerFactoryMgr::setFactory(const std::string& name, CertificateHandlerFactory* pFactory)
{
	bool success = _factories.insert(make_pair(name, Poco::SharedPtr<CertificateHandlerFactory>(pFactory))).second;
	if (!success)
		delete pFactory;
	poco_assert(success);
}
		

bool CertificateHandlerFactoryMgr::hasFactory(const std::string& name) const
{
	return _factories.find(name) != _factories.end();
}
		
	
const CertificateHandlerFactory* CertificateHandlerFactoryMgr::getFactory(const std::string& name) const
{
	FactoriesMap::const_iterator it = _factories.find(name);
	if (it != _factories.end())
		return it->second;
	else
		return 0;
}


void CertificateHandlerFactoryMgr::removeFactory(const std::string& name)
{
	_factories.erase(name);
}


} } // namespace Poco::Net
