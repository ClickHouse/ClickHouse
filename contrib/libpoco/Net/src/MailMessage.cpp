//
// MailMessage.cpp
//
// $Id: //poco/1.4/Net/src/MailMessage.cpp#2 $
//
// Library: Net
// Package: Mail
// Module:  MailMessage
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MediaType.h"
#include "Poco/Net/MultipartReader.h"
#include "Poco/Net/MultipartWriter.h"
#include "Poco/Net/PartSource.h"
#include "Poco/Net/PartHandler.h"
#include "Poco/Net/StringPartSource.h"
#include "Poco/Net/QuotedPrintableEncoder.h"
#include "Poco/Net/QuotedPrintableDecoder.h"
#include "Poco/Net/NameValueCollection.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Base64Decoder.h"
#include "Poco/StreamCopier.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeParser.h"
#include "Poco/String.h"
#include "Poco/StringTokenizer.h"
#include "Poco/StreamCopier.h"
#include "Poco/NumberFormatter.h"
#include <sstream>


using Poco::Base64Encoder;
using Poco::Base64Decoder;
using Poco::StreamCopier;
using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::DateTimeParser;
using Poco::StringTokenizer;
using Poco::icompare;


namespace Poco {
namespace Net {


namespace
{
	class MultiPartHandler: public PartHandler
		/// This is a default part handler for multipart messages, used when there 
		/// is no external handler provided to he MailMessage. This handler
		/// will handle all types of message parts, including attachments.
	{
	public:
		MultiPartHandler(MailMessage* pMsg): _pMsg(pMsg)
			/// Creates multi part handler.
			/// The pMsg pointer points to the calling MailMessage
			/// and will be used to properly populate it, so the
			/// message content could be written out unmodified
			/// in its entirety, including attachments.
		{
		}
		
		~MultiPartHandler()
			/// Destroys string part handler.
		{
		}
		
		void handlePart(const MessageHeader& header, std::istream& stream)
			/// Handles a part. If message pointer was provided at construction time, 
			/// the message pointed to will be properly populated so it could be written
			/// back out at a later point in time.
		{
			std::string tmp;
			Poco::StreamCopier::copyToString(stream, tmp);
			if (_pMsg)
			{
				MailMessage::ContentTransferEncoding cte = MailMessage::ENCODING_7BIT;
				if (header.has(MailMessage::HEADER_CONTENT_TRANSFER_ENCODING))
				{
					std::string enc = header[MailMessage::HEADER_CONTENT_TRANSFER_ENCODING];
					if (enc == MailMessage::CTE_8BIT)
						cte = MailMessage::ENCODING_8BIT;
					else if (enc == MailMessage::CTE_QUOTED_PRINTABLE)
						cte = MailMessage::ENCODING_QUOTED_PRINTABLE;
					else if (enc == MailMessage::CTE_BASE64)
						cte = MailMessage::ENCODING_BASE64;
				}

				NameValueCollection::ConstIterator it = header.begin();
				NameValueCollection::ConstIterator end = header.end();
				PartSource* pPS = _pMsg->createPartStore(tmp, 
					header[MailMessage::HEADER_CONTENT_TYPE], 
					getFileNameFromDisp(it->second));
				poco_check_ptr (pPS);
				for (; it != end; ++it)
				{
					if (MailMessage::HEADER_CONTENT_DISPOSITION == it->first)
					{
						if (it->second == "inline") _pMsg->addContent(pPS, cte);
						else _pMsg->addAttachment("", pPS, cte);
					}
					
					pPS->headers().set(it->first, it->second);
				}
			}
		}
		
	private:
		std::string getFileNameFromDisp(const std::string& str)
		{
			StringTokenizer st(str, ";=", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
			StringTokenizer::Iterator it = st.begin();
			StringTokenizer::Iterator end = st.end();
			for (; it != end; ++it) { if (*it == "filename") break; }
			if (it != end)
			{
				++it;
				if (it == end) return "";
				return *it;
			}
			return "";
		}

		MailMessage* _pMsg;
	};


	class StringPartHandler: public PartHandler
		/// This is a default part handler, used when there is no
		/// external handler provided to the MailMessage. This handler
		/// handles only single-part messages.
	{
	public:
		StringPartHandler(std::string& content): _str(content)
			/// Creates string part handler.
			/// The content parameter represents the part content.
		{
		}
		
		~StringPartHandler()
			/// Destroys string part handler.
		{
		}
		
		void handlePart(const MessageHeader& header, std::istream& stream)
			/// Handles a part.
		{
			std::string tmp;
			Poco::StreamCopier::copyToString(stream, tmp);
			_str.append(tmp);
		}
		
	private:
		std::string& _str;
	};
}


const std::string MailMessage::HEADER_SUBJECT("Subject");
const std::string MailMessage::HEADER_FROM("From");
const std::string MailMessage::HEADER_TO("To");
const std::string MailMessage::HEADER_CC("CC");
const std::string MailMessage::HEADER_BCC("BCC");
const std::string MailMessage::HEADER_DATE("Date");
const std::string MailMessage::HEADER_CONTENT_TYPE("Content-Type");
const std::string MailMessage::HEADER_CONTENT_TRANSFER_ENCODING("Content-Transfer-Encoding");
const std::string MailMessage::HEADER_CONTENT_DISPOSITION("Content-Disposition");
const std::string MailMessage::HEADER_CONTENT_ID("Content-ID");
const std::string MailMessage::HEADER_MIME_VERSION("Mime-Version");
const std::string MailMessage::EMPTY_HEADER;
const std::string MailMessage::TEXT_PLAIN("text/plain");
const std::string MailMessage::CTE_7BIT("7bit");
const std::string MailMessage::CTE_8BIT("8bit");
const std::string MailMessage::CTE_QUOTED_PRINTABLE("quoted-printable");
const std::string MailMessage::CTE_BASE64("base64");


MailMessage::MailMessage(PartStoreFactory* pStoreFactory): 
	_pStoreFactory(pStoreFactory)
{
	Poco::Timestamp now;
	setDate(now);
	setContentType("text/plain");
}


MailMessage::~MailMessage()
{
	for (PartVec::iterator it = _parts.begin(); it != _parts.end(); ++it)
	{
		delete it->pSource;
	}
}

	
void MailMessage::addRecipient(const MailRecipient& recipient)
{
	_recipients.push_back(recipient);
}


void MailMessage::setRecipients(const Recipients& recipients)
{
	_recipients.assign(recipients.begin(), recipients.end());
}


void MailMessage::setSender(const std::string& sender)
{
	set(HEADER_FROM, sender);
}

	
const std::string& MailMessage::getSender() const
{
	if (has(HEADER_FROM))
		return get(HEADER_FROM);
	else
		return EMPTY_HEADER;
}

	
void MailMessage::setSubject(const std::string& subject)
{
	set(HEADER_SUBJECT, subject);
}

	
const std::string& MailMessage::getSubject() const
{
	if (has(HEADER_SUBJECT))
		return get(HEADER_SUBJECT);
	else
		return EMPTY_HEADER;
}


void MailMessage::setContent(const std::string& content, ContentTransferEncoding encoding)
{
	_content  = content;
	_encoding = encoding;
	set(HEADER_CONTENT_TRANSFER_ENCODING, contentTransferEncodingToString(encoding));
}


void MailMessage::setContentType(const std::string& mediaType)
{
	set(HEADER_CONTENT_TYPE, mediaType);
}

	
void MailMessage::setContentType(const MediaType& mediaType)
{
	setContentType(mediaType.toString());
}

	
const std::string& MailMessage::getContentType() const
{
	if (has(HEADER_CONTENT_TYPE))
		return get(HEADER_CONTENT_TYPE);
	else
		return TEXT_PLAIN;
}


void MailMessage::setDate(const Poco::Timestamp& dateTime)
{
	set(HEADER_DATE, DateTimeFormatter::format(dateTime, DateTimeFormat::RFC1123_FORMAT));
}

	
Poco::Timestamp MailMessage::getDate() const
{
	const std::string& dateTime = get(HEADER_DATE);
	int tzd;
	return DateTimeParser::parse(dateTime, tzd).timestamp();
}

	
bool MailMessage::isMultipart() const
{
	MediaType mediaType = getContentType();
	return mediaType.matches("multipart");
}


void MailMessage::addPart(const std::string& name, PartSource* pSource, ContentDisposition disposition, ContentTransferEncoding encoding)
{
	poco_check_ptr (pSource);

	makeMultipart();
	Part part;
	part.name        = name;
	part.pSource     = pSource;
	part.disposition = disposition;
	part.encoding    = encoding;
	_parts.push_back(part);
}


void MailMessage::addContent(PartSource* pSource, ContentTransferEncoding encoding)
{
	addPart("", pSource, CONTENT_INLINE, encoding);
}

	
void MailMessage::addAttachment(const std::string& name, PartSource* pSource, ContentTransferEncoding encoding)
{
	addPart(name, pSource, CONTENT_ATTACHMENT, encoding);
}


void MailMessage::read(std::istream& istr, PartHandler& handler)
{
	readHeader(istr);
	if (isMultipart())
	{
		readMultipart(istr, handler);
	}
	else
	{
		StringPartHandler handler(_content);
		readPart(istr, *this, handler);
	}
}


void MailMessage::read(std::istream& istr)
{
	readHeader(istr);
	if (isMultipart())
	{
		MultiPartHandler handler(this);
		readMultipart(istr, handler);
	}
	else
	{
		StringPartHandler handler(_content);
		readPart(istr, *this, handler);
	}
}


void MailMessage::write(std::ostream& ostr) const
{
	MessageHeader header(*this);
	setRecipientHeaders(header);
	if (isMultipart())
	{
		writeMultipart(header, ostr);
	}
	else
	{
		writeHeader(header, ostr);
		std::istringstream istr(_content);
		writeEncoded(istr, ostr, _encoding);
	}
}


void MailMessage::makeMultipart()
{
	if (!isMultipart())
	{
		MediaType mediaType("multipart", "mixed");
		setContentType(mediaType);	
	}
}


void MailMessage::writeHeader(const MessageHeader& header, std::ostream& ostr) const
{
	header.write(ostr);
	ostr << "\r\n";
}


void MailMessage::writeMultipart(MessageHeader& header, std::ostream& ostr) const
{
	if (_boundary.empty()) _boundary = MultipartWriter::createBoundary();
	MediaType mediaType(getContentType());
	mediaType.setParameter("boundary", _boundary);
	header.set(HEADER_CONTENT_TYPE, mediaType.toString());
	header.set(HEADER_MIME_VERSION, "1.0");
	writeHeader(header, ostr);
	
	MultipartWriter writer(ostr, _boundary);
	for (PartVec::const_iterator it = _parts.begin(); it != _parts.end(); ++it)
	{
		writePart(writer, *it);
	}
	writer.close();
}


void MailMessage::writePart(MultipartWriter& writer, const Part& part) const
{
	MessageHeader partHeader(part.pSource->headers());
	MediaType mediaType(part.pSource->mediaType());
	if (!part.name.empty())
		mediaType.setParameter("name", part.name);
	partHeader.set(HEADER_CONTENT_TYPE, mediaType.toString());
	partHeader.set(HEADER_CONTENT_TRANSFER_ENCODING, contentTransferEncodingToString(part.encoding));
	std::string disposition;
	if (part.disposition == CONTENT_ATTACHMENT)
	{
		disposition = "attachment";
		const std::string& filename = part.pSource->filename();
		if (!filename.empty())
		{
			disposition.append("; filename=");
			quote(filename, disposition);
		}
	}
	else disposition = "inline";
	partHeader.set(HEADER_CONTENT_DISPOSITION, disposition);
	writer.nextPart(partHeader);
	writeEncoded(part.pSource->stream(), writer.stream(), part.encoding);
}


void MailMessage::writeEncoded(std::istream& istr, std::ostream& ostr, ContentTransferEncoding encoding) const
{
	switch (encoding)
	{
	case ENCODING_7BIT:
	case ENCODING_8BIT:
		StreamCopier::copyStream(istr, ostr);
		break;
	case ENCODING_QUOTED_PRINTABLE:
		{
			QuotedPrintableEncoder encoder(ostr);
			StreamCopier::copyStream(istr, encoder);
			encoder.close();
		}
		break;
	case ENCODING_BASE64:
		{
			Base64Encoder encoder(ostr);
			StreamCopier::copyStream(istr, encoder);
			encoder.close();
		}
		break;
	}
}


void MailMessage::readHeader(std::istream& istr)
{
	clear();
	MessageHeader::read(istr);
	istr.get(); // \r
	if ('\n' == istr.peek()) istr.get(); // \n
}


void MailMessage::readMultipart(std::istream& istr, PartHandler& handler)
{
	MediaType contentType(getContentType());
	_boundary = contentType.getParameter("boundary");
	MultipartReader reader(istr, _boundary);
	while (reader.hasNextPart())
	{
		MessageHeader partHeader;
		reader.nextPart(partHeader);
		readPart(reader.stream(), partHeader, handler);
	}
}


void MailMessage::readPart(std::istream& istr, const MessageHeader& header, PartHandler& handler)
{
	std::string encoding;
	if (header.has(HEADER_CONTENT_TRANSFER_ENCODING))
	{
		encoding = header.get(HEADER_CONTENT_TRANSFER_ENCODING);
		// get rid of a parameter if one is set
		std::string::size_type pos = encoding.find(';');
		if (pos != std::string::npos)
			encoding.resize(pos);
	}
	if (icompare(encoding, CTE_QUOTED_PRINTABLE) == 0)
	{
		QuotedPrintableDecoder decoder(istr);
		handlePart(decoder, header, handler);
	}
	else if (icompare(encoding, CTE_BASE64) == 0)
	{
		Base64Decoder decoder(istr);
		handlePart(decoder, header, handler);
	}
	else
	{
		handlePart(istr, header, handler);
	}
}


void MailMessage::handlePart(std::istream& istr, const MessageHeader& header, PartHandler& handler)
{
	handler.handlePart(header, istr);
	// Read remaining characters from stream in case
	// the handler failed to read the complete stream.
	while (istr.good()) istr.get();
}


void MailMessage::setRecipientHeaders(MessageHeader& headers) const
{
	std::string to;
	std::string cc;
	std::string bcc;
	
	for (Recipients::const_iterator it = _recipients.begin(); it != _recipients.end(); ++it)
	{
		switch (it->getType())
		{
		case MailRecipient::PRIMARY_RECIPIENT:
			appendRecipient(*it, to);
			break;
		case MailRecipient::CC_RECIPIENT:
			appendRecipient(*it, cc);
			break;
		case MailRecipient::BCC_RECIPIENT:
			break;
		}
	}
	if (!to.empty()) headers.set(HEADER_TO, to);
	if (!cc.empty()) headers.set(HEADER_CC, cc);
}


const std::string& MailMessage::contentTransferEncodingToString(ContentTransferEncoding encoding)
{
	switch (encoding)
	{
	case ENCODING_7BIT:
		return CTE_7BIT;
	case ENCODING_8BIT:
		return CTE_8BIT;
	case ENCODING_QUOTED_PRINTABLE:
		return CTE_QUOTED_PRINTABLE;
	case ENCODING_BASE64:
		return CTE_BASE64;
	default:
		poco_bugcheck();
	}
	return CTE_7BIT;
}


int MailMessage::lineLength(const std::string& str)
{
	int n = 0;
	std::string::const_reverse_iterator it  = str.rbegin();
	std::string::const_reverse_iterator end = str.rend();
	while (it != end && *it != '\n') { ++n; ++it; }
	return n;
}


void MailMessage::appendRecipient(const MailRecipient& recipient, std::string& str)
{
	if (!str.empty()) str.append(", ");
	const std::string& realName = recipient.getRealName();
	const std::string& address  = recipient.getAddress();
	std::string rec;
	if (!realName.empty())
	{
		quote(realName, rec, true);
		rec.append(" ");
	}
	rec.append("<");
	rec.append(address);
	rec.append(">");
	if (lineLength(str) + rec.length() > 70) str.append("\r\n\t");
	str.append(rec);
}


std::string MailMessage::encodeWord(const std::string& text, const std::string& charset)
{
	bool containsNonASCII = false;
	for (std::string::const_iterator it = text.begin(); it != text.end(); ++it)
	{
		if (static_cast<unsigned char>(*it) > 127)
		{
			containsNonASCII = true;
			break;
		}
	}
	if (!containsNonASCII) return text;
	
	std::string encodedText;
	std::string::size_type lineLength = 0;
	for (std::string::const_iterator it = text.begin(); it != text.end(); ++it)
	{
		if (lineLength == 0)
		{
			encodedText += "=?";
			encodedText += charset;
			encodedText += "?q?";
			lineLength += charset.length() + 5;
		}
		switch (*it)
		{
		case ' ':
			encodedText += '_';
			lineLength++;
			break;
		case '=':
		case '?':
		case '_':
		case '(':
		case ')':
		case '[':
		case ']':
		case '<':
		case '>':
		case ',':
		case ';':
		case ':':
		case '.':
		case '@':
			encodedText += '=';
			NumberFormatter::appendHex(encodedText, static_cast<unsigned>(static_cast<unsigned char>(*it)), 2);
			lineLength += 3;
			break;
		default:
			if (*it > 32 && *it < 127)
			{
				encodedText += *it;
				lineLength++;
			}
			else
			{
				encodedText += '=';
				NumberFormatter::appendHex(encodedText, static_cast<unsigned>(static_cast<unsigned char>(*it)), 2);
				lineLength += 3;
			}
		}
		if ((lineLength >= 64 && (*it == ' ' || *it == '\t' || *it == '\r' || *it == '\n')) || lineLength >= 72)
		{
			encodedText += "?=\r\n ";
			lineLength = 0;
		}
	}
	if (lineLength > 0)
	{
		encodedText += "?=";
	}	
	return encodedText;
}


PartSource* MailMessage::createPartStore(const std::string& content, const std::string& mediaType, const std::string& filename)
{
	if (!_pStoreFactory) return new StringPartSource(content, mediaType, filename);
	else return _pStoreFactory->createPartStore(content, mediaType, filename);
}


} } // namespace Poco::Net
