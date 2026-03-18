//
// FTPClientSession.cpp
//
// Library: Net
// Package: FTP
// Module:  FTPClientSession
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/FTPClientSession.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/NetException.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Ascii.h"


using Poco::NumberFormatter;


namespace Poco {
namespace Net {


FTPClientSession::FTPClientSession():
	_port(0),
	_pControlSocket(0),
	_pDataStream(0),
	_passiveMode(true),
	_fileType(TYPE_BINARY),
	_supports1738(true),
	_serverReady(false),
	_isLoggedIn(false),
	_timeout(DEFAULT_TIMEOUT)
{
}

	
FTPClientSession::FTPClientSession(const StreamSocket& socket):
	_host(socket.address().host().toString()),
	_port(socket.address().port()),
	_pControlSocket(new DialogSocket(socket)),
	_pDataStream(0),
	_passiveMode(true),
	_fileType(TYPE_BINARY),
	_supports1738(true),
	_serverReady(false),
	_isLoggedIn(false),
	_timeout(DEFAULT_TIMEOUT)
{
	_pControlSocket->setReceiveTimeout(_timeout);
}


FTPClientSession::FTPClientSession(const std::string& host,
	Poco::UInt16 port,
	const std::string& username,
	const std::string& password):
	_host(host),
	_port(port),
	_pControlSocket(new DialogSocket(SocketAddress(host, port))),
	_pDataStream(0),
	_passiveMode(true),
	_fileType(TYPE_BINARY),
	_supports1738(true),
	_serverReady(false),
	_isLoggedIn(false),
	_timeout(DEFAULT_TIMEOUT)
{
	if (!username.empty())
		login(username, password);
	else
		_pControlSocket->setReceiveTimeout(_timeout);
}


FTPClientSession::~FTPClientSession()
{
	try
	{
		close();
	}
	catch (...) 
	{
	}
}


void FTPClientSession::setTimeout(const Poco::Timespan& timeout)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	_timeout = timeout;
	_pControlSocket->setReceiveTimeout(timeout);
}

	
Poco::Timespan FTPClientSession::getTimeout() const
{
	return _timeout;
}


void FTPClientSession::setPassive(bool flag, bool useRFC1738)
{
	_passiveMode  = flag;
	_supports1738 = useRFC1738;
}

	
bool FTPClientSession::getPassive() const
{
	return _passiveMode;
}


void FTPClientSession::open(const std::string& host,
	Poco::UInt16 port,
	const std::string& username,
	const std::string& password)
{
	_host = host;
	_port = port;
	if (!username.empty())
	{
		login(username, password);
	}
	else
	{
		_pControlSocket = new DialogSocket(SocketAddress(_host, _port));
		_pControlSocket->setReceiveTimeout(_timeout);
	}
}


void FTPClientSession::login(const std::string& username, const std::string& password)
{
	if (_isLoggedIn) logout();

	int status = FTP_POSITIVE_COMPLETION * 100;
	std::string response;
	if (!_pControlSocket)
	{
		_pControlSocket = new DialogSocket(SocketAddress(_host, _port));
		_pControlSocket->setReceiveTimeout(_timeout);
	}

	if (!_serverReady)
	{
		status = _pControlSocket->receiveStatusMessage(response);
		if (!isPositiveCompletion(status))
			throw FTPException("Cannot login to server", response, status);

		_serverReady = true;
	}

	status = sendCommand("USER", username, response);
	if (isPositiveIntermediate(status))
		status = sendCommand("PASS", password, response);
	if (!isPositiveCompletion(status)) 
		throw FTPException("Login denied", response, status);

	setFileType(_fileType);
	_isLoggedIn = true;
}


void FTPClientSession::logout()
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	if (_isLoggedIn)
	{
		try { endTransfer(); }
		catch (...) { }
		_isLoggedIn = false;
		std::string response;
		sendCommand("QUIT", response);
	}
}


void FTPClientSession::close()
{
	try { logout(); }
	catch (...) {}
	_serverReady = false;
	if (_pControlSocket)
	{
		_pControlSocket->close();
		delete _pControlSocket;
		_pControlSocket = 0;
	}
}


void FTPClientSession::setFileType(FTPClientSession::FileType type)
{
	std::string response;
	int status = sendCommand("TYPE", (type == TYPE_TEXT ? "A" : "I"), response);
	if (!isPositiveCompletion(status)) throw FTPException("Cannot set file type", response, status);
	_fileType = type;
}


FTPClientSession::FileType FTPClientSession::getFileType() const
{
	return _fileType;
}


std::string FTPClientSession::systemType()
{
	std::string response;
	int status = sendCommand("SYST", response);
	if (isPositiveCompletion(status))
		return response.substr(4);
	else
		throw FTPException("Cannot get remote system type", response, status);
}


void FTPClientSession::setWorkingDirectory(const std::string& path)
{
	std::string response;
	int status = sendCommand("CWD", path, response);
	if (!isPositiveCompletion(status))
		throw FTPException("Cannot change directory", response, status);
}


std::string FTPClientSession::getWorkingDirectory()
{
	std::string response;
	int status = sendCommand("PWD", response);
	if (isPositiveCompletion(status))
		return extractPath(response);
	else
		throw FTPException("Cannot get current working directory", response, status);
}


void FTPClientSession::cdup()
{
	std::string response;
	int status = sendCommand("CDUP", response);
	if (!isPositiveCompletion(status))
		throw FTPException("Cannot change directory", response, status);
}

	
void FTPClientSession::rename(const std::string& oldName, const std::string& newName)
{
	std::string response;
	int status = sendCommand("RNFR", oldName, response);
	if (!isPositiveIntermediate(status)) 
		throw FTPException(std::string("Cannot rename ") + oldName, response, status);
	status = sendCommand("RNTO", newName, response);
	if (!isPositiveCompletion(status)) 
		throw FTPException(std::string("Cannot rename to ") + newName, response, status);
}

	
void FTPClientSession::remove(const std::string& path)
{
	std::string response;
	int status = sendCommand("DELE", path, response);
	if (!isPositiveCompletion(status)) 
		throw FTPException(std::string("Cannot remove " + path), response, status);
}


void FTPClientSession::createDirectory(const std::string& path)
{
	std::string response;
	int status = sendCommand("MKD", path, response);
	if (!isPositiveCompletion(status)) 
		throw FTPException(std::string("Cannot create directory ") + path, response, status);
}


void FTPClientSession::removeDirectory(const std::string& path)
{
	std::string response;
	int status = sendCommand("RMD", path, response);
	if (!isPositiveCompletion(status)) 
		throw FTPException(std::string("Cannot remove directory ") + path, response, status);
}


std::istream& FTPClientSession::beginDownload(const std::string& path)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	delete _pDataStream;
	_pDataStream = 0;
	_pDataStream = new SocketStream(establishDataConnection("RETR", path));
	return *_pDataStream;
}

	
void FTPClientSession::endDownload()
{
	endTransfer();
}

	
std::ostream& FTPClientSession::beginUpload(const std::string& path)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	delete _pDataStream;
	_pDataStream = 0;
	_pDataStream = new SocketStream(establishDataConnection("STOR", path));
	return *_pDataStream;
}


void FTPClientSession::endUpload()
{
	endTransfer();
}


std::istream& FTPClientSession::beginList(const std::string& path, bool extended)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	delete _pDataStream;
	_pDataStream = 0;
	_pDataStream = new SocketStream(establishDataConnection(extended ? "LIST" : "NLST", path));
	return *_pDataStream;
}


void FTPClientSession::endList()
{
	endTransfer();
}

	
void FTPClientSession::abort()
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	_pControlSocket->sendByte(DialogSocket::TELNET_IP);
	_pControlSocket->synch();
	std::string response;
	int status = sendCommand("ABOR", response);
	if (status == 426)
		status = _pControlSocket->receiveStatusMessage(response);
	if (status != 226) 
		throw FTPException("Cannot abort transfer", response, status);
}


int FTPClientSession::sendCommand(const std::string& command, std::string& response)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	_pControlSocket->sendMessage(command);
	return _pControlSocket->receiveStatusMessage(response);
}


int FTPClientSession::sendCommand(const std::string& command, const std::string& arg, std::string& response)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	_pControlSocket->sendMessage(command, arg);
	return _pControlSocket->receiveStatusMessage(response);
}


std::string FTPClientSession::extractPath(const std::string& response)
{
	std::string path;
	std::string::const_iterator it  = response.begin();
	std::string::const_iterator end = response.end();
	while (it != end && *it != '"') ++it;
	if (it != end)
	{
		++it;
		while (it != end)
		{
			if (*it == '"')
			{
				++it;
				if (it == end || (it != end && *it != '"')) break;
			}
			path += *it++;
		}
	}
	return path;
}


StreamSocket FTPClientSession::establishDataConnection(const std::string& command, const std::string& arg)
{
	if (_passiveMode)
		return passiveDataConnection(command, arg);
	else
		return activeDataConnection(command, arg);
}


StreamSocket FTPClientSession::activeDataConnection(const std::string& command, const std::string& arg)
{
	if (!isOpen())
		throw FTPException("Connection is closed.");

	ServerSocket server(SocketAddress(_pControlSocket->address().host(), 0));
	sendPortCommand(server.address());
	std::string response;
	int status = sendCommand(command, arg, response);
	if (!isPositivePreliminary(status)) 
		throw FTPException(command + " command failed", response, status);
	if (server.poll(_timeout, Socket::SELECT_READ))
		return server.acceptConnection();
	else
		throw FTPException("The server has not initiated a data connection");
}


StreamSocket FTPClientSession::passiveDataConnection(const std::string& command, const std::string& arg)
{
	SocketAddress sa(sendPassiveCommand());
	StreamSocket sock(sa);
	std::string response;
	int status = sendCommand(command, arg, response);
	if (!isPositivePreliminary(status)) 
		throw FTPException(command + " command failed", response, status);
	return sock;
}


void FTPClientSession::sendPortCommand(const SocketAddress& addr)
{
	if (_supports1738)
	{
		if (sendEPRT(addr))
			return;
		else
			_supports1738 = false;
	}
	sendPORT(addr);
}


SocketAddress FTPClientSession::sendPassiveCommand()
{
	SocketAddress addr;
	if (_supports1738)
	{
		if (sendEPSV(addr))
			return addr;
		else
			_supports1738 = false;
	}
	sendPASV(addr);
	return addr;
}


bool FTPClientSession::sendEPRT(const SocketAddress& addr)
{
	std::string arg("|");
	arg += addr.af() == AF_INET ? '1' : '2';
	arg += '|';
	arg += addr.host().toString();
	arg += '|';
	arg += NumberFormatter::format(addr.port());
	arg += '|';
	std::string response;
	int status = sendCommand("EPRT", arg, response);
	if (isPositiveCompletion(status))
		return true;
	else if (isPermanentNegative(status))
		return false;
	else
		throw FTPException("EPRT command failed", response, status);
}


void FTPClientSession::sendPORT(const SocketAddress& addr)
{
	std::string arg(addr.host().toString());
	for (std::string::iterator it = arg.begin(); it != arg.end(); ++it)
	{
		if (*it == '.') *it = ',';
	}
	arg += ',';
	Poco::UInt16 port = addr.port();
	arg += NumberFormatter::format(port/256);
	arg += ',';
	arg += NumberFormatter::format(port % 256);
	std::string response;
	int status = sendCommand("PORT", arg, response);
	if (!isPositiveCompletion(status)) 
		throw FTPException("PORT command failed", response, status);
}


bool FTPClientSession::sendEPSV(SocketAddress& addr)
{
	std::string response;
	int status = sendCommand("EPSV", response);
	if (isPositiveCompletion(status))
	{
		parseExtAddress(response, addr);
		return true;
	}
	else if (isPermanentNegative(status))
	{
		return false;
	}
	else throw FTPException("EPSV command failed", response, status);
}


void FTPClientSession::sendPASV(SocketAddress& addr)
{
	std::string response;
	int status = sendCommand("PASV", response);
	if (!isPositiveCompletion(status)) 
		throw FTPException("PASV command failed", response, status);
	parseAddress(response, addr);
}


void FTPClientSession::parseAddress(const std::string& str, SocketAddress& addr)
{
	std::string::const_iterator it  = str.begin();
	std::string::const_iterator end = str.end();
	while (it != end && *it != '(') ++it;
	if (it != end) ++it;
	std::string host;
	while (it != end && Poco::Ascii::isDigit(*it)) host += *it++;
	if (it != end && *it == ',') { host += '.'; ++it; }
	while (it != end && Poco::Ascii::isDigit(*it)) host += *it++;
	if (it != end && *it == ',') { host += '.'; ++it; }
	while (it != end && Poco::Ascii::isDigit(*it)) host += *it++;
	if (it != end && *it == ',') { host += '.'; ++it; }
	while (it != end && Poco::Ascii::isDigit(*it)) host += *it++;
	if (it != end && *it == ',') ++it;
	Poco::UInt16 portHi = 0;
	while (it != end && Poco::Ascii::isDigit(*it)) { portHi *= 10; portHi += *it++ - '0'; }
	if (it != end && *it == ',') ++it;
	Poco::UInt16 portLo = 0;
	while (it != end && Poco::Ascii::isDigit(*it)) { portLo *= 10; portLo += *it++ - '0'; }
	addr = SocketAddress(host, portHi*256 + portLo);
}


void FTPClientSession::parseExtAddress(const std::string& str, SocketAddress& addr)
{
	std::string::const_iterator it  = str.begin();
	std::string::const_iterator end = str.end();
	while (it != end && *it != '(') ++it;
	if (it != end) ++it;
	char delim = '|';
	if (it != end) delim = *it++;
	if (it != end && *it == delim) ++it;
	if (it != end && *it == delim) ++it;
	Poco::UInt16 port = 0;
	while (it != end && Poco::Ascii::isDigit(*it)) { port *= 10; port += *it++ - '0'; }	
	addr = SocketAddress(_pControlSocket->peerAddress().host(), port);	
}


void FTPClientSession::endTransfer()
{
	if (_pDataStream)
	{
		delete _pDataStream;
		_pDataStream = 0;
		std::string response;
		int status = _pControlSocket->receiveStatusMessage(response);
		if (!isPositiveCompletion(status)) 
			throw FTPException("Data transfer failed", response, status);
	}
}


} } // namespace Poco::Net
