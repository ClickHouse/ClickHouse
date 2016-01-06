//
// SMTPChannel.cpp
//
// $Id: //poco/svn/Net/src/SMTPChannel.cpp#2 $
//
// Library: Net
// Package: Logging
// Module:  SMTPChannel
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SMTPChannel.h"
#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MailRecipient.h"
#include "Poco/Net/SMTPClientSession.h"
#include "Poco/Net/StringPartSource.h"
#include "Poco/Message.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/LocalDateTime.h"
#include "Poco/LoggingFactory.h"
#include "Poco/Instantiator.h"
#include "Poco/NumberFormatter.h"
#include "Poco/FileStream.h"
#include "Poco/File.h"
#include "Poco/Environment.h"


namespace Poco {
namespace Net {


const std::string SMTPChannel::PROP_MAILHOST("mailhost");
const std::string SMTPChannel::PROP_SENDER("sender");
const std::string SMTPChannel::PROP_RECIPIENT("recipient");
const std::string SMTPChannel::PROP_LOCAL("local");
const std::string SMTPChannel::PROP_ATTACHMENT("attachment");
const std::string SMTPChannel::PROP_TYPE("type");
const std::string SMTPChannel::PROP_DELETE("delete");
const std::string SMTPChannel::PROP_THROW("throw");


SMTPChannel::SMTPChannel():
	_mailHost("localhost"),
	_local(true),
	_type("text/plain"),
	_delete(false),
	_throw(false)
{
}

		
SMTPChannel::SMTPChannel(const std::string& mailhost, const std::string& sender, const std::string& recipient):
	_mailHost(mailhost),
	_sender(sender),
	_recipient(recipient),
	_local(true),
	_type("text/plain"),
	_delete(false),
	_throw(false)
{
}


SMTPChannel::~SMTPChannel()
{
	try
	{
		close();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void SMTPChannel::open()
{
}

	
void SMTPChannel::close()
{
}

	
void SMTPChannel::log(const Message& msg)
{
	try
	{
		MailMessage message;
		message.setSender(_sender);
		message.addRecipient(MailRecipient(MailRecipient::PRIMARY_RECIPIENT, _recipient));
		message.setSubject("Log Message from " + _sender);
		std::stringstream content;
		content << "Log Message\r\n"
			<< "===========\r\n\r\n"
			<< "Host: " << Environment::nodeName() << "\r\n"
			<< "Logger: " << msg.getSource() << "\r\n";

		if (_local)
		{
			DateTime dt(msg.getTime());
			content	<< "Timestamp: " << DateTimeFormatter::format(LocalDateTime(dt), DateTimeFormat::RFC822_FORMAT) << "\r\n";
		}
		else
			content	<< "Timestamp: " << DateTimeFormatter::format(msg.getTime(), DateTimeFormat::RFC822_FORMAT) << "\r\n";

		content	<< "Priority: " << NumberFormatter::format(msg.getPriority()) << "\r\n"
			<< "Process ID: " << NumberFormatter::format(msg.getPid()) << "\r\n"
			<< "Thread: " << msg.getThread() << " (ID: " << msg.getTid() << ")\r\n"
			<< "Message text: " << msg.getText() << "\r\n\r\n";

		message.addContent(new StringPartSource(content.str()));
	
		if (!_attachment.empty())
		{
			{
				Poco::FileInputStream fis(_attachment, std::ios::in | std::ios::binary | std::ios::ate);
				if (fis.good())
				{
					typedef std::allocator<std::string::value_type>::size_type SST;

					std::streamoff size = fis.tellg();
					poco_assert (std::numeric_limits<unsigned int>::max() >= size);
					poco_assert (std::numeric_limits<SST>::max() >= size);
					char* pMem = new char [static_cast<unsigned int>(size)];
					fis.seekg(std::ios::beg);
					fis.read(pMem, size);
					message.addAttachment(_attachment,
						new StringPartSource(std::string(pMem, static_cast<SST>(size)), 
							_type,
							_attachment));

					delete [] pMem;
				}
			}
			if (_delete) File(_attachment).remove();
		}

		SMTPClientSession session(_mailHost);
		session.login();
		session.sendMessage(message);
		session.close();
	} 
	catch (Exception&) 
	{ 
		if (_throw) throw; 
	}
}

	
void SMTPChannel::setProperty(const std::string& name, const std::string& value)
{
	if (name == PROP_MAILHOST) 
		_mailHost = value;
	else if (name == PROP_SENDER) 
		_sender = value;
	else if (name == PROP_RECIPIENT) 
		_recipient = value;
	else if (name == PROP_LOCAL) 
		_local = isTrue(value);
	else if (name == PROP_ATTACHMENT) 
		_attachment = value;
	else if (name == PROP_TYPE) 
		_type = value;
	else if (name == PROP_DELETE) 
		_delete = isTrue(value);
	else if (name == PROP_THROW) 
		_throw = isTrue(value);
	else 
		Channel::setProperty(name, value);
}

	
std::string SMTPChannel::getProperty(const std::string& name) const
{
	if (name == PROP_MAILHOST) 
		return _mailHost;
	else if (name == PROP_SENDER) 
		return _sender;
	else if (name == PROP_RECIPIENT) 
		return _recipient;
	else if (name == PROP_LOCAL) 
		return _local ? "true" : "false";
	else if (name == PROP_ATTACHMENT) 
		return _attachment;
	else if (name == PROP_TYPE) 
		return _type;
	else if (name == PROP_DELETE) 
		return _delete ? "true" : "false";
	else if (name == PROP_THROW) 
		return _throw ? "true" : "false";
	else
		return Channel::getProperty(name);
}


void SMTPChannel::registerChannel()
{
	Poco::LoggingFactory::defaultFactory().registerChannelClass("SMTPChannel", 
		new Poco::Instantiator<SMTPChannel, Poco::Channel>);
}


} } // namespace Poco::Net
