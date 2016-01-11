//
// FTPStreamFactory.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/FTPStreamFactory.h#1 $
//
// Library: Net
// Package: FTP
// Module:  FTPStreamFactory
//
// Definition of the FTPStreamFactory class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_FTPStreamFactory_INCLUDED
#define Net_FTPStreamFactory_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPSession.h"
#include "Poco/URIStreamFactory.h"


namespace Poco {
namespace Net {


class Net_API FTPPasswordProvider
	/// The base class for all password providers.
	/// An instance of a subclass of this class can be
	/// registered with the FTPStreamFactory to 
	/// provide a password
{
public:
	virtual std::string password(const std::string& username, const std::string& host) = 0;
		/// Provide the password for the given user on the given host.

protected:
	FTPPasswordProvider();
	virtual ~FTPPasswordProvider();
};


class Net_API FTPStreamFactory: public Poco::URIStreamFactory
	/// An implementation of the URIStreamFactory interface
	/// that handles File Transfer Protocol (ftp) URIs.
	///
	/// The URI's path may end with an optional type specification
	/// in the form (;type=<typecode>), where <typecode> is
	/// one of a, i or d. If type=a, the file identified by the path
	/// is transferred in ASCII (text) mode. If type=i, the file
	/// is transferred in Image (binary) mode. If type=d, a directory
	/// listing (in NLST format) is returned. This corresponds with
	/// the FTP URL format specified in RFC 1738.
	///
	/// If the URI does not contain a username and password, the
	/// username "anonymous" and the password "
{
public:
	FTPStreamFactory();
		/// Creates the FTPStreamFactory.

	~FTPStreamFactory();
		/// Destroys the FTPStreamFactory.
		
	std::istream* open(const Poco::URI& uri);
		/// Creates and opens a HTTP stream for the given URI.
		/// The URI must be a ftp://... URI.
		///
		/// Throws a NetException if anything goes wrong.
		
	static void setAnonymousPassword(const std::string& password);
		/// Sets the password used for anonymous FTP.
		///
		/// WARNING: Setting the anonymous password is not
		/// thread-safe, so it's best to call this method
		/// during application initialization, before the
		/// FTPStreamFactory is used for the first time.
		
	static const std::string& getAnonymousPassword();
		/// Returns the password used for anonymous FTP.
		
	static void setPasswordProvider(FTPPasswordProvider* pProvider);
		/// Sets the FTPPasswordProvider. If NULL is given,
		/// no password provider is used.
		///
		/// WARNING: Setting the password provider is not
		/// thread-safe, so it's best to call this method
		/// during application initialization, before the
		/// FTPStreamFactory is used for the first time.
		
	static FTPPasswordProvider* getPasswordProvider();
		/// Returns the FTPPasswordProvider currently in use,
		/// or NULL if no one has been set.

	static void registerFactory();
		/// Registers the FTPStreamFactory with the
		/// default URIStreamOpener instance.

	static void unregisterFactory();
		/// Unregisters the FTPStreamFactory with the
		/// default URIStreamOpener instance.

protected:
	static void splitUserInfo(const std::string& userInfo, std::string& username, std::string& password);
	static void getUserInfo(const Poco::URI& uri, std::string& username, std::string& password);
	static void getPathAndType(const Poco::URI& uri, std::string& path, char& type);
	
private:
	static std::string          _anonymousPassword;
	static FTPPasswordProvider* _pPasswordProvider;
};


} } // namespace Poco::Net


#endif // Net_FTPStreamFactory_INCLUDED
