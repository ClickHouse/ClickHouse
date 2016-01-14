//
// LogRotation.cpp
//
// $Id: LogRotation.cpp 580 2008-06-01 20:56:01Z ryppka $
//
// This class demonstrates the Log rotation feature.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/AutoPtr.h"
#include "Poco/ConsoleChannel.h"
#include "Poco/SplitterChannel.h"
#include "Poco/FileChannel.h"
#include "Poco/PatternFormatter.h"
#include "Poco/FormattingChannel.h"
#include "Poco/Message.h"
#include "Poco/Logger.h"
#include <iostream>
#include <sstream>


using Poco::AutoPtr;
using Poco::Channel;
using Poco::ConsoleChannel;
using Poco::SplitterChannel;
using Poco::FileChannel;
using Poco::FormattingChannel;
using Poco::Formatter;
using Poco::PatternFormatter;
using Poco::Logger;
using Poco::Message;


int main (int, char**)
{
	AutoPtr<SplitterChannel> splitterChannel(new SplitterChannel());

	AutoPtr<Channel> consoleChannel(new ConsoleChannel());
	AutoPtr<Channel> fileChannel(new FileChannel("test.log"));
	AutoPtr<FileChannel> rotatedFileChannel(new FileChannel("rotated.log"));

	rotatedFileChannel->setProperty("rotation", "100");
	rotatedFileChannel->setProperty("archive", "timestamp");

	splitterChannel->addChannel(consoleChannel);
	splitterChannel->addChannel(fileChannel);
	splitterChannel->addChannel(rotatedFileChannel);


	AutoPtr<Formatter> formatter(new PatternFormatter("%h-%M-%S.%i: %t"));
	AutoPtr<Channel> formattingChannel(new FormattingChannel(formatter, splitterChannel));

	Logger& logger = Logger::create("TestLog", formattingChannel, Message::PRIO_TRACE);

	for (int i = 0; i < 10; ++i)
	{
		std::ostringstream oss;
		oss << "Value of i: " << i;
		logger.fatal(oss.str());
	}

	return 0;
}
