//
// MailRecipient.cpp
//
// Library: Net
// Package: Mail
// Module:  MailRecipient
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MailRecipient.h"
#include <algorithm>


namespace Poco {
namespace Net {


MailRecipient::MailRecipient():
	_type(PRIMARY_RECIPIENT)
{
}

	
MailRecipient::MailRecipient(const MailRecipient& recipient):
	_address(recipient._address),
	_realName(recipient._realName),
	_type(recipient._type)
{
}

	
MailRecipient::MailRecipient(RecipientType type, const std::string& address):
	_address(address),
	_type(type)
{
}


MailRecipient::MailRecipient(RecipientType type, const std::string& address, const std::string& realName):
	_address(address),
	_realName(realName),
	_type(type)
{
}


MailRecipient::~MailRecipient()
{
}

	
MailRecipient& MailRecipient::operator = (const MailRecipient& recipient)
{
	if (this != &recipient)
	{
		MailRecipient tmp(recipient);
		swap(tmp);
	}
	return *this;
}


void MailRecipient::swap(MailRecipient& recipient)
{
	std::swap(_type, recipient._type);
	std::swap(_address, recipient._address);
	std::swap(_realName, recipient._realName);
}

	
void MailRecipient::setType(RecipientType type)
{
	_type = type;
}

	
void MailRecipient::setAddress(const std::string& address)
{
	_address = address;
}

	
void MailRecipient::setRealName(const std::string& realName)
{
	_realName = realName;
}


} } // namespace Poco::Net
