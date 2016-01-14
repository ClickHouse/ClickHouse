//
// Mail.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/samples/Mail/src/Mail.cpp#1 $
//
// This sample demonstrates the MailMessage and SecureSMTPClientSession classes.
//
// Copyright (c) 2005-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MailRecipient.h"
#include "Poco/Net/SecureSMTPClientSession.h"
#include "Poco/Net/StringPartSource.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/KeyConsoleHandler.h"
#include "Poco/Net/ConsoleCertificateHandler.h"
#include "Poco/SharedPtr.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::Net::MailMessage;
using Poco::Net::MailRecipient;
using Poco::Net::SMTPClientSession;
using Poco::Net::SecureSMTPClientSession;
using Poco::Net::StringPartSource;
using Poco::Net::SSLManager;
using Poco::Net::Context;
using Poco::Net::KeyConsoleHandler;
using Poco::Net::PrivateKeyPassphraseHandler;
using Poco::Net::InvalidCertificateHandler;
using Poco::Net::ConsoleCertificateHandler;
using Poco::SharedPtr;
using Poco::Path;
using Poco::Exception;


class SSLInitializer
{
public:
	SSLInitializer()
	{
		Poco::Net::initializeSSL();
	}
	
	~SSLInitializer()
	{
		Poco::Net::uninitializeSSL();
	}
};


const unsigned char PocoLogo[] =
{
	#include "PocoLogo.hpp"
};


int main(int argc, char** argv)
{
	SSLInitializer sslInitializer;

	if (argc < 4)
	{
		Path p(argv[0]);
		std::cerr << "usage: " << p.getBaseName() << " <mailhost> <sender> <recipient> [<username> <password>]" << std::endl;
		std::cerr << "       Send an email greeting from <sender> to <recipient>," << std::endl;
		std::cerr << "       using a secure connection to the SMTP server at <mailhost>." << std::endl;
		return 1;
	}
	
	std::string mailhost(argv[1]);
	std::string sender(argv[2]);
	std::string recipient(argv[3]);
	std::string username(argc >= 5 ? argv[4] : "");
	std::string password(argc >= 6 ? argv[5] : "");
	
	try
	{
		// Note: we must create the passphrase handler prior Context 
		SharedPtr<InvalidCertificateHandler> pCert = new ConsoleCertificateHandler(false); // ask the user via console
		Context::Ptr pContext = new Context(Context::CLIENT_USE, "", "", "", Context::VERIFY_RELAXED, 9, true, "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
		SSLManager::instance().initializeClient(0, pCert, pContext);

		MailMessage message;
		message.setSender(sender);
		message.addRecipient(MailRecipient(MailRecipient::PRIMARY_RECIPIENT, recipient));
		message.setSubject("Hello from the POCO C++ Libraries");
		std::string content;
		content += "Hello ";
		content += recipient;
		content += ",\r\n\r\n";
		content += "This is a greeting from the POCO C++ Libraries.\r\n\r\n";
		std::string logo(reinterpret_cast<const char*>(PocoLogo), sizeof(PocoLogo));
		message.addContent(new StringPartSource(content));
		message.addAttachment("logo", new StringPartSource(logo, "image/gif"));
		
		SecureSMTPClientSession session(mailhost);
		session.login();
		session.startTLS(pContext);
		if (!username.empty())
		{
			session.login(SMTPClientSession::AUTH_LOGIN, username, password);
		}
		session.sendMessage(message);
		session.close();
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
	return 0;
}
