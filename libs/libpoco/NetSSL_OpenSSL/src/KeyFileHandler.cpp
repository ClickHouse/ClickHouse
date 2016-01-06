//
// KeyFileHandler.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/KeyFileHandler.cpp#1 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  KeyFileHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/KeyFileHandler.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/File.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/OptionException.h"


namespace Poco {
namespace Net {


const std::string KeyFileHandler::CFG_PRIV_KEY_FILE("privateKeyPassphraseHandler.options.password");


KeyFileHandler::KeyFileHandler(bool server):PrivateKeyPassphraseHandler(server)
{
}


KeyFileHandler::~KeyFileHandler()
{
}


void KeyFileHandler::onPrivateKeyRequested(const void* pSender, std::string& privateKey)
{
	try
	{
		Poco::Util::AbstractConfiguration& config = Poco::Util::Application::instance().config();
		std::string prefix = serverSide() ? SSLManager::CFG_SERVER_PREFIX : SSLManager::CFG_CLIENT_PREFIX;
		if (!config.hasProperty(prefix + CFG_PRIV_KEY_FILE))
			throw Poco::Util::EmptyOptionException(std::string("Missing Configuration Entry: ") + prefix + CFG_PRIV_KEY_FILE);
		
		privateKey = config.getString(prefix + CFG_PRIV_KEY_FILE);
	}
	catch (Poco::NullPointerException&)
	{
		throw Poco::IllegalStateException(
			"An application configuration is required to obtain the private key passphrase, "
			"but no Poco::Util::Application instance is available."
			);
	}
}


} } // namespace Poco::Net
