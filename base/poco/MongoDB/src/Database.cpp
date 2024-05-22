//
// Database.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Database
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Database.h"
#include "Poco/MongoDB/Binary.h"
#include "Poco/MD5Engine.h"
#include "Poco/SHA1Engine.h"
#include "Poco/PBKDF2Engine.h"
#include "Poco/HMACEngine.h"
#include "Poco/Base64Decoder.h"
#include "Poco/MemoryStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/Exception.h"
#include "Poco/RandomStream.h"
#include "Poco/Random.h"
#include "Poco/Format.h"
#include "Poco/NumberParser.h"
#include <sstream>
#include <map>


namespace Poco {
namespace MongoDB {


const std::string Database::AUTH_MONGODB_CR("MONGODB-CR");
const std::string Database::AUTH_SCRAM_SHA1("SCRAM-SHA-1");


namespace
{
	std::map<std::string, std::string> parseKeyValueList(const std::string& str)
	{
		std::map<std::string, std::string> kvm;
		std::string::const_iterator it = str.begin();
		std::string::const_iterator end = str.end();
		while (it != end)
		{
			std::string k;
			std::string v;
			while (it != end && *it != '=') k += *it++;
			if (it != end) ++it;
			while (it != end && *it != ',') v += *it++;
			if (it != end) ++it;
			kvm[k] = v;
		}
		return kvm;
	}

	std::string decodeBase64(const std::string& base64)
	{
		Poco::MemoryInputStream istr(base64.data(), base64.size());
		Poco::Base64Decoder decoder(istr);
		std::string result;
		Poco::StreamCopier::copyToString(decoder, result);
		return result;
	}

	std::string encodeBase64(const std::string& data)
	{
		std::ostringstream ostr;
		Poco::Base64Encoder encoder(ostr);
		encoder.rdbuf()->setLineLength(0);
		encoder << data;
		encoder.close();
		return ostr.str();
	}

	std::string digestToBinaryString(Poco::DigestEngine& engine)
	{
		Poco::DigestEngine::Digest d = engine.digest();
		return std::string(reinterpret_cast<const char*>(&d[0]), d.size());
	}

	std::string digestToHexString(Poco::DigestEngine& engine)
	{
		Poco::DigestEngine::Digest d = engine.digest();
		return Poco::DigestEngine::digestToHex(d);
	}

	std::string digestToBase64(Poco::DigestEngine& engine)
	{
		return encodeBase64(digestToBinaryString(engine));
	}

	std::string hashCredentials(const std::string& username, const std::string& password)
	{
		Poco::MD5Engine md5;
		md5.update(username);
		md5.update(std::string(":mongo:"));
		md5.update(password);
		return digestToHexString(md5);
	}

	std::string createNonce()
	{
		Poco::MD5Engine md5;
		Poco::RandomInputStream randomStream;
		Poco::Random random;
		for (int i = 0; i < 4; i++)
		{
			md5.update(randomStream.get());
			md5.update(random.nextChar());
		}
		return digestToHexString(md5);
	}
}


Database::Database(const std::string& db):
	_dbname(db)
{
}


Database::~Database()
{
}


bool Database::authenticate(Connection& connection, const std::string& username, const std::string& password, const std::string& method)
{
	if (username.empty()) throw Poco::InvalidArgumentException("empty username");
	if (password.empty()) throw Poco::InvalidArgumentException("empty password");

	if (method == AUTH_MONGODB_CR)
		return authCR(connection, username, password);
	else if (method == AUTH_SCRAM_SHA1)
		return authSCRAM(connection, username, password);
	else
		throw Poco::InvalidArgumentException("authentication method", method);
}


bool Database::authCR(Connection& connection, const std::string& username, const std::string& password)
{
	std::string nonce;
	Poco::SharedPtr<QueryRequest> pCommand = createCommand();
	pCommand->selector().add<Poco::Int32>("getnonce", 1);

	ResponseMessage response;
	connection.sendRequest(*pCommand, response);
	if (response.documents().size() > 0)
	{
		Document::Ptr pDoc = response.documents()[0];
		if (pDoc->getInteger("ok") != 1) return false;
		nonce = pDoc->get<std::string>("nonce", "");
		if (nonce.empty()) throw Poco::ProtocolException("no nonce received");
	}
	else throw Poco::ProtocolException("empty response for getnonce");

	std::string credsDigest = hashCredentials(username, password);

	Poco::MD5Engine md5;
	md5.update(nonce);
	md5.update(username);
	md5.update(credsDigest);
	std::string key = digestToHexString(md5);

	pCommand = createCommand();
	pCommand->selector()
		.add<Poco::Int32>("authenticate", 1)
		.add<std::string>("user", username)
		.add<std::string>("nonce", nonce)
		.add<std::string>("key", key);

	connection.sendRequest(*pCommand, response);
	if (response.documents().size() > 0)
	{
		Document::Ptr pDoc = response.documents()[0];
		return pDoc->getInteger("ok") == 1;
	}
	else throw Poco::ProtocolException("empty response for authenticate");
}


bool Database::authSCRAM(Connection& connection, const std::string& username, const std::string& password)
{
	std::string clientNonce(createNonce());
	std::string clientFirstMsg = Poco::format("n=%s,r=%s", username, clientNonce);

	Poco::SharedPtr<QueryRequest> pCommand = createCommand();
	pCommand->selector()
		.add<Poco::Int32>("saslStart", 1)
		.add<std::string>("mechanism", AUTH_SCRAM_SHA1)
		.add<Binary::Ptr>("payload", new Binary(Poco::format("n,,%s", clientFirstMsg)));

	ResponseMessage response;
	connection.sendRequest(*pCommand, response);

	Int32 conversationId = 0;
	std::string serverFirstMsg;

	if (response.documents().size() > 0)
	{
		Document::Ptr pDoc = response.documents()[0];
		if (pDoc->getInteger("ok") == 1)
		{
			Binary::Ptr pPayload = pDoc->get<Binary::Ptr>("payload");
			serverFirstMsg = pPayload->toRawString();
			conversationId = pDoc->get<Int32>("conversationId");
		}
		else
		{
			if (pDoc->exists("errmsg"))
			{
				const Poco::MongoDB::Element::Ptr value = pDoc->get("errmsg");
				auto message = static_cast<const Poco::MongoDB::ConcreteElement<std::string> &>(*value).value();
				throw Poco::RuntimeException(message);
			}
			else
				return false;
		}
	}
	else throw Poco::ProtocolException("empty response for saslStart");

	std::map<std::string, std::string> kvm = parseKeyValueList(serverFirstMsg);
	const std::string serverNonce = kvm["r"];
	const std::string salt = decodeBase64(kvm["s"]);
	const unsigned iterations = Poco::NumberParser::parseUnsigned(kvm["i"]);
	const Poco::UInt32 dkLen = 20;

	std::string hashedPassword = hashCredentials(username, password);

	Poco::PBKDF2Engine<Poco::HMACEngine<Poco::SHA1Engine> > pbkdf2(salt, iterations, dkLen);
	pbkdf2.update(hashedPassword);
	std::string saltedPassword = digestToBinaryString(pbkdf2);

	std::string clientFinalNoProof = Poco::format("c=biws,r=%s", serverNonce);
	std::string authMessage = Poco::format("%s,%s,%s", clientFirstMsg, serverFirstMsg, clientFinalNoProof);

	Poco::HMACEngine<Poco::SHA1Engine> hmacKey(saltedPassword);
	hmacKey.update(std::string("Client Key"));
	std::string clientKey = digestToBinaryString(hmacKey);

	Poco::SHA1Engine sha1;
	sha1.update(clientKey);
	std::string storedKey = digestToBinaryString(sha1);

	Poco::HMACEngine<Poco::SHA1Engine> hmacSig(storedKey);
	hmacSig.update(authMessage);
	std::string clientSignature = digestToBinaryString(hmacSig);

	std::string clientProof(clientKey);
	for (std::size_t i = 0; i < clientProof.size(); i++)
	{
		clientProof[i] ^= clientSignature[i];
	}

	std::string clientFinal = Poco::format("%s,p=%s", clientFinalNoProof, encodeBase64(clientProof));

	pCommand = createCommand();
	pCommand->selector()
		.add<Poco::Int32>("saslContinue", 1)
		.add<Poco::Int32>("conversationId", conversationId)
		.add<Binary::Ptr>("payload", new Binary(clientFinal));

	std::string serverSecondMsg;
	connection.sendRequest(*pCommand, response);
	if (response.documents().size() > 0)
	{
		Document::Ptr pDoc = response.documents()[0];
		if (pDoc->getInteger("ok") == 1)
		{
			Binary::Ptr pPayload = pDoc->get<Binary::Ptr>("payload");
			serverSecondMsg = pPayload->toRawString();
		}
		else
		{
			if (pDoc->exists("errmsg"))
			{
				const Poco::MongoDB::Element::Ptr value = pDoc->get("errmsg");
				auto message = static_cast<const Poco::MongoDB::ConcreteElement<std::string> &>(*value).value();
				throw Poco::RuntimeException(message);
			}
			else
				return false;
		}
	}
	else throw Poco::ProtocolException("empty response for saslContinue");

	Poco::HMACEngine<Poco::SHA1Engine> hmacSKey(saltedPassword);
	hmacSKey.update(std::string("Server Key"));
	std::string serverKey = digestToBinaryString(hmacSKey);

	Poco::HMACEngine<Poco::SHA1Engine> hmacSSig(serverKey);
	hmacSSig.update(authMessage);
	std::string serverSignature = digestToBase64(hmacSSig);

	kvm = parseKeyValueList(serverSecondMsg);
	std::string serverSignatureReceived = kvm["v"];

	if (serverSignature != serverSignatureReceived)
		throw Poco::ProtocolException("server signature verification failed");

	pCommand = createCommand();
	pCommand->selector()
		.add<Poco::Int32>("saslContinue", 1)
		.add<Poco::Int32>("conversationId", conversationId)
		.add<Binary::Ptr>("payload", new Binary);

	connection.sendRequest(*pCommand, response);
	if (response.documents().size() > 0)
	{
		Document::Ptr pDoc = response.documents()[0];
		if (pDoc->getInteger("ok") == 1)
		{
			return true;
		}
		else
		{
			if (pDoc->exists("errmsg"))
			{
				const Poco::MongoDB::Element::Ptr value = pDoc->get("errmsg");
				auto message = static_cast<const Poco::MongoDB::ConcreteElement<std::string> &>(*value).value();
				throw Poco::RuntimeException(message);
			}
			else
				return false;
		}
	}
	else throw Poco::ProtocolException("empty response for saslContinue");
}


Document::Ptr Database::queryBuildInfo(Connection& connection) const
{
	// build info can be issued on "config" system database
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = createCommand();
	request->selector().add("buildInfo", 1);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*request, response);

	Document::Ptr buildInfo;
	if ( response.documents().size() > 0 )
	{
		buildInfo = response.documents()[0];
	}
	else
	{
		throw Poco::ProtocolException("Didn't get a response from the buildinfo command");
	}
	return buildInfo;
}


Document::Ptr Database::queryServerHello(Connection& connection, bool old) const
{
	// hello can be issued on "config" system database
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = createCommand();

	// 'hello' command was previously called 'isMaster'
	std::string command_name;
	if (old)
		command_name = "isMaster";
	else
		command_name = "hello";

	request->selector().add(command_name, 1);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*request, response);

	Document::Ptr hello;
	if ( response.documents().size() > 0 )
	{
		hello = response.documents()[0];
	}
	else
	{
		throw Poco::ProtocolException("Didn't get a response from the hello command");
	}
	return hello;
}


Int64 Database::count(Connection& connection, const std::string& collectionName) const
{
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> countRequest = createCountRequest(collectionName);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*countRequest, response);

	if (response.documents().size() > 0)
	{
		Poco::MongoDB::Document::Ptr doc = response.documents()[0];
		return doc->getInteger("n");
	}

	return -1;
}


Poco::MongoDB::Document::Ptr Database::ensureIndex(Connection& connection, const std::string& collection, const std::string& indexName, Poco::MongoDB::Document::Ptr keys, bool unique, bool background, int version, int ttl)
{
	Poco::MongoDB::Document::Ptr index = new Poco::MongoDB::Document();
	index->add("ns", _dbname + "." + collection);
	index->add("name", indexName);
	index->add("key", keys);

	if (version > 0)
	{
		index->add("version", version);
	}

	if (unique)
	{
		index->add("unique", true);
	}

	if (background)
	{
		index->add("background", true);
	}

	if (ttl > 0)
	{
		index->add("expireAfterSeconds", ttl);
	}

	Poco::SharedPtr<Poco::MongoDB::InsertRequest> insertRequest = createInsertRequest("system.indexes");
	insertRequest->documents().push_back(index);
	connection.sendRequest(*insertRequest);

	return getLastErrorDoc(connection);
}


Document::Ptr Database::getLastErrorDoc(Connection& connection) const
{
	Document::Ptr errorDoc;

	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = createCommand();
	request->setNumberToReturn(1);
	request->selector().add("getLastError", 1);

	Poco::MongoDB::ResponseMessage response;
	connection.sendRequest(*request, response);

	if (response.documents().size() > 0)
	{
		errorDoc = response.documents()[0];
	}

	return errorDoc;
}


std::string Database::getLastError(Connection& connection) const
{
	Document::Ptr errorDoc = getLastErrorDoc(connection);
	if (!errorDoc.isNull() && errorDoc->isType<std::string>("err"))
	{
		return errorDoc->get<std::string>("err");
	}

	return "";
}


Poco::SharedPtr<Poco::MongoDB::QueryRequest> Database::createCountRequest(const std::string& collectionName) const
{
	Poco::SharedPtr<Poco::MongoDB::QueryRequest> request = createCommand();
	request->setNumberToReturn(1);
	request->selector().add("count", collectionName);
	return request;
}


} } // namespace Poco::MongoDB
