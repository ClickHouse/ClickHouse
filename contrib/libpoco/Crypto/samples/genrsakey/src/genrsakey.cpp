//
// genrsakey.cpp
//
// $Id: //poco/1.4/Crypto/samples/genrsakey/src/genrsakey.cpp#1 $
//
// This sample demonstrates the XYZ class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Application.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionException.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/AutoPtr.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/String.h"
#include "Poco/Crypto/RSAKey.h"
#include <iostream>


using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;
using Poco::Util::AbstractConfiguration;
using Poco::Util::OptionCallback;
using Poco::AutoPtr;
using Poco::NumberParser;
using Poco::Crypto::RSAKey;


class RSAApp: public Application
	/// This sample demonstrates some of the features of the Util::Application class,
	/// such as configuration file handling and command line arguments processing.
	///
	/// Try genrsakey --help (on Unix platforms) or genrsakey /help (elsewhere) for
	/// more information.
{
public:
	RSAApp():
	  _helpRequested(false),
		 _length(RSAKey::KL_1024),
		 _exp(RSAKey::EXP_LARGE),
		 _name(),
		 _pwd()
	{
		Poco::Crypto::initializeCrypto();
	}
	
	~RSAApp()
	{
		Poco::Crypto::uninitializeCrypto();
	}

protected:	
	void initialize(Application& self)
	{
		loadConfiguration(); // load default configuration files, if present
		Application::initialize(self);
	}
	
	void uninitialize()
	{
		Application::uninitialize();
	}
	
	void reinitialize(Application& self)
	{
		Application::reinitialize(self);
	}
	
	void defineOptions(OptionSet& options)
	{
		Application::defineOptions(options);

		options.addOption(
			Option("help", "h", "display help information on command line arguments")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<RSAApp>(this, &RSAApp::handleHelp)));

		options.addOption(
			Option("?", "?", "display help information on command line arguments")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<RSAApp>(this, &RSAApp::handleHelp)));

		options.addOption(
			Option("key", "k", "define the key length")
				.required(false)
				.repeatable(false)
				.argument("512|1024|2048|4096")
				.callback(OptionCallback<RSAApp>(this, &RSAApp::handleKeyLength)));
				
		options.addOption(
			Option("exponent", "e", "defines the exponent of the key")
				.required(false)
				.repeatable(false)
				.argument("small|large")
				.callback(OptionCallback<RSAApp>(this, &RSAApp::handleExponent)));

		options.addOption(
			Option("file", "f", "defines the file base name. creates a file.pub and a file.priv")
				.required(true)
				.repeatable(false)
				.argument("filebasename")
				.callback(OptionCallback<RSAApp>(this, &RSAApp::handleFilePrefix)));

		options.addOption(
			Option("password", "p", "defines the password used to encrypt the private key file. If not defined user will be asked via stdin to provide in")
				.required(false)
				.repeatable(false)
				.argument("pwd")
				.callback(OptionCallback<RSAApp>(this, &RSAApp::handlePassword)));
	}
	
	void handleHelp(const std::string& name, const std::string& value)
	{
		_helpRequested = true;
		displayHelp();
		stopOptionsProcessing();
	}
	
	void handleKeyLength(const std::string& name, const std::string& value)
	{
		int keyLen = Poco::NumberParser::parse(value);
		if (keyLen == 512 || keyLen == 1024 || keyLen == 2048 || keyLen == 4096)
			_length = (RSAKey::KeyLength)keyLen;
		else
			throw Poco::Util::IncompatibleOptionsException("Illegal key length value");
	}

	void handleExponent(const std::string& name, const std::string& value)
	{
		if (Poco::icompare(value, "small") == 0)
			_exp = RSAKey::EXP_SMALL;
		else
			_exp = RSAKey::EXP_LARGE;
	}

	void handleFilePrefix(const std::string& name, const std::string& value)
	{
		if (value.empty())
			throw Poco::Util::IncompatibleOptionsException("Empty file prefix forbidden");
		_name = value;
	}
	
	void handlePassword(const std::string& name, const std::string& value)
	{
		_pwd = value;
	}
		
	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("OPTIONS");
		helpFormatter.setHeader("Application for generating RSA public/private key pairs.");
		helpFormatter.format(std::cout);
	}

	int main(const std::vector<std::string>& args)
	{
		if (!_helpRequested)
		{
			logger().information("Generating key with length " + Poco::NumberFormatter::format((int)_length));
			logger().information(std::string("Exponent is ") + ((_exp == RSAKey::EXP_SMALL)?"small":"large"));
			logger().information("Generating key");
			RSAKey key(_length, _exp);
			logger().information("Generating key: DONE");
			std::string pubFile(_name + ".pub");
			std::string privFile(_name + ".priv");
			
			logger().information("Saving key to " + pubFile + " and " + privFile);
			key.save(pubFile, privFile, _pwd);
			logger().information("Key saved");
		}
		return Application::EXIT_OK;
	}
	
private:
	bool _helpRequested;
	RSAKey::KeyLength _length;
	RSAKey::Exponent  _exp;
	std::string       _name;
	std::string       _pwd;
};


POCO_APP_MAIN(RSAApp)
