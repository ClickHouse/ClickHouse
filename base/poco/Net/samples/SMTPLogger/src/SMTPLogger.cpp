//
// Mail.cpp
//
// This sample demonstrates the SMTPChannel class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MailMessage.h"
#include "Poco/Net/SMTPChannel.h"
#include "Poco/Logger.h"
#include "Poco/Path.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::Net::SMTPChannel;
using Poco::Logger;
using Poco::Path;
using Poco::AutoPtr;
using Poco::Exception;


#if defined(POCO_OS_FAMILY_UNIX)
const std::string fileName = "${POCO_BASE}/Net/samples/SMTPLogger/res/logo.gif";
#elif defined(POCO_OS_FAMILY_WINDOWS)
const std::string fileName = "%POCO_BASE%/Net/samples/SMTPLogger/res/logo.gif";
#endif


int main(int argc, char** argv)
{
	if (argc != 4)
	{
		Path p(argv[0]);
		std::cerr << "usage: " << p.getBaseName() << " <mailhost> <sender> <recipient>" << std::endl;
		std::cerr << "       Send an email log entry from <sender> to <recipient>," << std::endl;
		std::cerr << "       the SMTP server at <mailhost>." << std::endl;
		return 1;
	}
	
	std::string mailhost(argv[1]);
	std::string sender(argv[2]);
	std::string recipient(argv[3]);
	std::string attachment = Path::expand(fileName);

	try
	{
		AutoPtr<SMTPChannel> pSMTPChannel = new SMTPChannel(mailhost, sender, recipient);
		pSMTPChannel->setProperty("attachment", attachment);
		pSMTPChannel->setProperty("type", "image/gif");
		pSMTPChannel->setProperty("delete", "false");
		pSMTPChannel->setProperty("throw", "true");

		Logger& logger = Logger::get("POCO Sample SMTP Logger");
		logger.setChannel(pSMTPChannel.get());
		logger.critical("Critical message");
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
	return 0;
}
