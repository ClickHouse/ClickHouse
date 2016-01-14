//
// MailRecipient.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/MailRecipient.h#1 $
//
// Library: Net
// Package: Mail
// Module:  MailRecipient
//
// Definition of the MailRecipient class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MailRecipient_INCLUDED
#define Net_MailRecipient_INCLUDED


#include "Poco/Net/Net.h"


namespace Poco {
namespace Net {


class Net_API MailRecipient
	/// The recipient of an e-mail message.
	///
	/// A recipient has a type (primary recipient,
	/// carbon-copy recipient, blind-carbon-copy
	/// recipient), an e-mail address and an optional
	/// real name.
{
public:
	enum RecipientType
	{
		PRIMARY_RECIPIENT,
		CC_RECIPIENT,
		BCC_RECIPIENT
	};

	MailRecipient();
		/// Creates an empty MailRecipient.
		
	MailRecipient(const MailRecipient& recipient);
		/// Creates a MailRecipient by copying another one.
		
	MailRecipient(RecipientType type, const std::string& address);
		/// Creates a MailRecipient of the given type.

	MailRecipient(RecipientType type, const std::string& address, const std::string& realName);
		/// Creates a MailRecipient of the given type.

	~MailRecipient();
		/// Destroys the MailRecipient.
		
	MailRecipient& operator = (const MailRecipient& recipient);
		/// Assigns another recipient.
		
	void swap(MailRecipient& recipient);
		/// Exchanges the content of two recipients.

	RecipientType getType() const;
		/// Returns the type of the recipient.
		
	void setType(RecipientType type);
		/// Sets the type of the recipient.
		
	const std::string& getAddress() const;
		/// Returns the address of the recipient.
		
	void setAddress(const std::string& address);
		/// Sets the address of the recipient.
		
	const std::string& getRealName() const;
		/// Returns the real name of the recipient.
		
	void setRealName(const std::string& realName);
		/// Sets the real name of the recipient.

private:
	std::string   _address;
	std::string   _realName;
	RecipientType _type;
};


//
// inlines
//
inline MailRecipient::RecipientType MailRecipient::getType() const
{
	return _type;
}


inline const std::string& MailRecipient::getAddress() const
{
	return _address;
}


inline const std::string& MailRecipient::getRealName() const
{
	return _realName;
}


inline void swap(MailRecipient& r1, MailRecipient& r2)
{
	r1.swap(r2);
}


} } // namespace Poco::Net


#endif // Net_MailRecipient_INCLUDED
