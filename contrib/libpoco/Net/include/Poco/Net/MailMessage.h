//
// MailMessage.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/MailMessage.h#2 $
//
// Library: Net
// Package: Mail
// Module:  MailMessage
//
// Definition of the MailMessage class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MailMessage_INCLUDED
#define Net_MailMessage_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/Net/MailRecipient.h"
#include "Poco/Net/PartStore.h"
#include "Poco/Timestamp.h"
#include <vector>


namespace Poco {
namespace Net {


class MediaType;
class PartSource;
class PartHandler;
class MultipartWriter;


class Net_API MailMessage: public MessageHeader
	/// This class represents an e-mail message for
	/// use with the SMTPClientSession and POPClientSession
	/// classes.
	///
	/// MailMessage supports both old-style plain text messages,
	/// as well as MIME multipart mail messages with attachments.
	///
	/// For multi-part messages, the following content transfer
	/// encodings are supported: 7bit, 8bit, quoted-printable
	/// and base64.
{
public:
	typedef std::vector<MailRecipient> Recipients;
	
	enum ContentDisposition
	{
		CONTENT_INLINE,
		CONTENT_ATTACHMENT
	};
	
	enum ContentTransferEncoding
	{
		ENCODING_7BIT,
		ENCODING_8BIT,
		ENCODING_QUOTED_PRINTABLE,
		ENCODING_BASE64
	};

	struct Part
	{
		std::string             name;
		PartSource*             pSource;
		ContentDisposition      disposition;
		ContentTransferEncoding encoding;
	};
	
	typedef std::vector<Part> PartVec;

	MailMessage(PartStoreFactory* pStoreFactory = 0);
		/// Creates an empty MailMessage.
		/// 
		/// If pStoreFactory is not null, message attachments will be 
		/// handled by the object created by the factory. Most
		/// common reason is to temporarily save attachments to 
		/// the file system in order to avoid potential memory 
		/// exhaustion when attachment files are very large.

	virtual ~MailMessage();
		/// Destroys the MailMessage.

	void addRecipient(const MailRecipient& recipient);
		/// Adds a recipient for the message.

	void setRecipients(const Recipients& recipient);
		/// Clears existing and sets new recipient list for the message.
		
	const Recipients& recipients() const;
		/// Returns the recipients of the message.

	void setSubject(const std::string& subject);
		/// Sets the subject of the message.
		///
		/// The subject must not contain any non-ASCII
		/// characters. To include non-ASCII characters
		/// in the subject, use RFC 2047 word encoding
		/// (see encodeWord()).
		
	const std::string& getSubject() const;
		/// Returns the subject of the message.
		
	void setSender(const std::string& sender);
		/// Sets the sender of the message (which
		/// ends up in the From header field).
		///
		/// The sender must either be a valid email
		/// address, or a real name followed by
		/// an email address enclosed in < and >.
		///
		/// The sender must not contain any non-ASCII
		/// characters. To include non-ASCII characters
		/// in the sender, use RFC 2047 word encoding
		/// (see encodeWord()).

	const std::string& getSender() const;
		/// Returns the sender of the message (taken
		/// from the From header field).

	void setContent(const std::string& content, ContentTransferEncoding encoding = ENCODING_QUOTED_PRINTABLE);
		/// Sets the content of the mail message.
		///
		/// If the content transfer encoding is ENCODING_7BIT or
		/// ENCODING_8BIT, the content string must be formatted
		/// according to the rules of an internet email message.
		///
		/// The message will be sent as a single-part
		/// message.
		///
		/// Note that single CR or LF characters as line delimiters must
		/// not be used. Content lines always should be terminated with a 
		/// proper CRLF sequence.
	
	const std::string& getContent() const;
		/// Returns the content of the mail message.
		///
		/// A content will only be returned for single-part
		/// messages. The content of multi-part mail messages
		/// will be reported through the registered PartHandler.
		
	void setContentType(const std::string& mediaType);
		/// Sets the content type for the message.
		
	void setContentType(const MediaType& mediaType);
		/// Sets the content type for the message.
		
	const std::string& getContentType() const;
		/// Returns the content type for the message.

	void setDate(const Poco::Timestamp& dateTime);
		/// Sets the Date header to the given date/time value.
		
	Poco::Timestamp getDate() const;
		/// Returns the value of the Date header.

	bool isMultipart() const;
		/// Returns true iff the message is a multipart message.

	void addPart(const std::string& name,
		PartSource* pSource,
		ContentDisposition disposition,
		ContentTransferEncoding encoding); 
		/// Adds a part/attachment to the mail message.
		///
		/// The MailMessage takes ownership of the PartSource and deletes it
		/// when it is no longer needed.
		///
		/// The MailMessage will be converted to a multipart message
		/// if it is not already one.
		///
		/// The part name, and the filename specified in the part source
		/// must not contain any non-ASCII characters.
		/// To include non-ASCII characters in the part name or filename, 
		/// use RFC 2047 word encoding (see encodeWord()).

	void addContent(PartSource* pSource,
		ContentTransferEncoding encoding = ENCODING_QUOTED_PRINTABLE);
		/// Adds a part to the mail message by calling
		/// addPart("", pSource, CONTENT_INLINE, encoding);
		///
		/// The part name, and the filename specified in the part source
		/// must not contain any non-ASCII characters.
		/// To include non-ASCII characters in the part name or filename, 
		/// use RFC 2047 word encoding (see encodeWord()).

	void addAttachment(const std::string& name,
		PartSource* pSource,
		ContentTransferEncoding encoding = ENCODING_BASE64);
		/// Adds an attachment to the mail message by calling
		/// addPart(name, pSource, CONTENT_ATTACHMENT, encoding);
		///
		/// The part name, and the filename specified in the part source
		/// must not contain any non-ASCII characters.
		/// To include non-ASCII characters in the part name or filename, 
		/// use RFC 2047 word encoding (see encodeWord()).

	PartSource* createPartStore(const std::string& content,
		const std::string& mediaType,
		const std::string& filename = "");
		/// Returns either default StringPartSource part store or, 
		/// if the part store factory was provided during contruction,
		/// the one created by PartStoreFactory.
		/// Returned part store is allocated on the heap; it is caller's 
		/// responsibility to delete it after use. Typical use is handler 
		/// passing it back to MailMessage, which takes care of the cleanup.

	const PartVec& parts() const;
		/// Returns const reference to the vector containing part stores.

	void read(std::istream& istr, PartHandler& handler);
		/// Reads the MailMessage from the given input stream.
		///
		/// If the message has multiple parts, the parts
		/// are reported to the PartHandler. If the message
		/// is not a multi-part message, the content is stored
		/// in a string available by calling getContent().

	void read(std::istream& istr);
		/// Reads the MailMessage from the given input stream.
		///
		/// The raw message (including all MIME parts) is stored
		/// in a string and available by calling getContent().

	void write(std::ostream& ostr) const;
		/// Writes the mail message to the given output stream.

	static std::string encodeWord(const std::string& text, const std::string& charset = "UTF-8");
		/// If the given string contains non-ASCII characters, 
		/// encodes the given string using RFC 2047 "Q" word encoding.
		/// 
		/// The given text must already be encoded in the character set
		/// given in charset (default is UTF-8).
		///
		/// Returns the encoded string, or the original string if it 
		/// consists only of ASCII characters.

	static const std::string HEADER_SUBJECT;
	static const std::string HEADER_FROM;
	static const std::string HEADER_TO;
	static const std::string HEADER_CC;
	static const std::string HEADER_BCC;
	static const std::string HEADER_DATE;
	static const std::string HEADER_CONTENT_TYPE;
	static const std::string HEADER_CONTENT_TRANSFER_ENCODING;
	static const std::string HEADER_CONTENT_DISPOSITION;
	static const std::string HEADER_CONTENT_ID;
	static const std::string HEADER_MIME_VERSION;
	static const std::string EMPTY_HEADER;
	static const std::string TEXT_PLAIN;
	static const std::string CTE_7BIT;
	static const std::string CTE_8BIT;
	static const std::string CTE_QUOTED_PRINTABLE;
	static const std::string CTE_BASE64;

protected:

	void makeMultipart();
	void writeHeader(const MessageHeader& header, std::ostream& ostr) const;
	void writeMultipart(MessageHeader& header, std::ostream& ostr) const;
	void writePart(MultipartWriter& writer, const Part& part) const;
	void writeEncoded(std::istream& istr, std::ostream& ostr, ContentTransferEncoding encoding) const;
	void setRecipientHeaders(MessageHeader& headers) const;
	void readHeader(std::istream& istr);
	void readMultipart(std::istream& istr, PartHandler& handler);
	void readPart(std::istream& istr, const MessageHeader& header, PartHandler& handler);
	void handlePart(std::istream& istr, const MessageHeader& header, PartHandler& handler);
	static const std::string& contentTransferEncodingToString(ContentTransferEncoding encoding);
	static int lineLength(const std::string& str);
	static void appendRecipient(const MailRecipient& recipient, std::string& str);

private:
	MailMessage(const MailMessage&);
	MailMessage& operator = (const MailMessage&);

	Recipients              _recipients;
	PartVec                 _parts;
	std::string             _content;
	ContentTransferEncoding _encoding;
	mutable std::string     _boundary;
	PartStoreFactory*       _pStoreFactory;
};


//
// inlines
//
inline const MailMessage::Recipients& MailMessage::recipients() const
{
	return _recipients;
}


inline const std::string& MailMessage::getContent() const
{
	return _content;
}


inline const MailMessage::PartVec& MailMessage::parts() const
{
	return _parts;
}


} } // namespace Poco::Net


#endif // Net_MailMessage_INCLUDED
