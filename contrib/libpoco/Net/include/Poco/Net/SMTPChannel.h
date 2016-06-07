//
// SMTPChannel.h
//
// $Id: //poco/svn/Net/include/Poco/Net/SMTPChannel.h#1 $
//
// Library: Net
// Package: Logging
// Module:  SMTPChannel
//
// Definition of the SMTPChannel class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SMTPChannel_INCLUDED
#define Net_SMTPChannel_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Channel.h"
#include "Poco/String.h"


namespace Poco {
namespace Net {


class Net_API SMTPChannel: public Poco::Channel
	/// This Channel implements SMTP (email) logging.
{
public:
	SMTPChannel();
		/// Creates a SMTPChannel.
		
	SMTPChannel(const std::string& mailhost, const std::string& sender, const std::string& recipient);
		/// Creates a SMTPChannel with the given target mailhost, sender, and recipient.
	
	void open();
		/// Opens the SMTPChannel.
		
	void close();
		/// Closes the SMTPChannel.
		
	void log(const Message& msg);
		/// Sends the message's text to the recipient.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given value.
		///
		/// The following properties are supported:
		///     * mailhost:   The SMTP server. Default is "localhost".
		///     * sender:     The sender address.
		///     * recipient:  The recipient address.
		///     * local:      If true, local time is used. Default is true.
		///     * attachment: Filename of the file to attach.
		///     * type:       Content type of the file to attach.
		///     * delete:     Boolean value indicating whether to delete 
		///                   the attachment file after sending.
		///     * throw:      Boolean value indicating whether to throw 
		///                   exception upon failure.
		
	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.

	static void registerChannel();
		/// Registers the channel with the global LoggingFactory.

	static const std::string PROP_MAILHOST;
	static const std::string PROP_SENDER;
	static const std::string PROP_RECIPIENT;
	static const std::string PROP_LOCAL;
	static const std::string PROP_ATTACHMENT;
	static const std::string PROP_TYPE;
	static const std::string PROP_DELETE;
	static const std::string PROP_THROW;

protected:
	~SMTPChannel();

private:
	bool isTrue(const std::string& value) const;

	std::string _mailHost;
	std::string _sender;
	std::string _recipient;
	bool        _local;
	std::string _attachment;
	std::string _type;
	bool        _delete;
	bool        _throw;
};


inline bool SMTPChannel::isTrue(const std::string& value) const
{
	return ((0 == icompare(value, "true")) ||
			(0 == icompare(value, "t")) ||
			(0 == icompare(value, "yes")) ||
			(0 == icompare(value, "y")));
}


} } // namespace Poco::Net


#endif // Net_SMTPChannel_INCLUDED
