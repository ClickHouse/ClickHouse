//
// unzip.cpp
//
// $Id: //poco/1.4/Zip/samples/unzip/src/unzip.cpp#1 $
//
// This sample demonstrates the Decompress class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Application.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/AutoPtr.h"
#include "Poco/Zip/Decompress.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipArchive.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/Delegate.h"
#include <iostream>
#include <fstream>


using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;
using Poco::Util::AbstractConfiguration;
using Poco::Util::OptionCallback;
using Poco::Zip::ZipLocalFileHeader;
using Poco::AutoPtr;


class DecompressHandler
{
public:
	DecompressHandler()
	{
	}

	~DecompressHandler()
	{
	}

	void onError(const void*, std::pair<const ZipLocalFileHeader, const std::string>& info)
	{
		Poco::Util::Application::instance().logger().error("ERR: " + info.second);
	}

	void onOk(const void*, std::pair<const ZipLocalFileHeader, const Poco::Path>& info)
	{
		Poco::Util::Application::instance().logger().information("OK: " + info.second.toString(Poco::Path::PATH_UNIX));
	}
};


class UnzipApp: public Application
	/// This sample demonstrates some of the features of the Util::Application class,
	/// such as configuration file handling and command line arguments processing.
	///
	/// Try zip --help (on Unix platforms) or zip /help (elsewhere) for
	/// more information.
{
public:
	UnzipApp(): _helpRequested(false)
	{
	}

protected:	
	void initialize(Application& self)
	{
		loadConfiguration(); // load default configuration files, if present
		Application::initialize(self);
		// add your own initialization code here
	}
	
	void uninitialize()
	{
		// add your own uninitialization code here
		Application::uninitialize();
	}
	
	void reinitialize(Application& self)
	{
		Application::reinitialize(self);
		// add your own reinitialization code here
	}
	
	void defineOptions(OptionSet& options)
	{
		Application::defineOptions(options);

		options.addOption(
			Option("help", "h", "display help information on command line arguments")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<UnzipApp>(this, &UnzipApp::handleHelp)));

		options.addOption(
			Option("file", "f", "specifies the input zip file")
				.required(true)
				.repeatable(false)
				.argument("filename")
				.callback(OptionCallback<UnzipApp>(this, &UnzipApp::handleFile)));
	}
	
	void handleHelp(const std::string& name, const std::string& value)
	{
		_helpRequested = true;
		displayHelp();
		stopOptionsProcessing();
	}

	void handleFile(const std::string& name, const std::string& value)
	{
		_zipFile = value;
	}
	
	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("OPTIONS outdir");
		helpFormatter.setHeader("A application that demonstrates usage of Poco::Zip::Decompress class.");
		helpFormatter.format(std::cout);
	}
	

	int main(const std::vector<std::string>& args)
	{
		if (!_helpRequested)
		{
			Poco::Path outputDir;
			if (!args.empty())
				outputDir.parseDirectory(args[0]);

			std::ifstream in(_zipFile.c_str(), std::ios::binary);
			Poco::Zip::Decompress c(in, outputDir);
			DecompressHandler handler;
			c.EError += Poco::Delegate<DecompressHandler, std::pair<const ZipLocalFileHeader, const std::string> >(&handler, &DecompressHandler::onError);
			c.EOk +=Poco::Delegate<DecompressHandler, std::pair<const ZipLocalFileHeader, const Poco::Path> >(&handler, &DecompressHandler::onOk);
			c.decompressAllFiles();
			c.EError -= Poco::Delegate<DecompressHandler, std::pair<const ZipLocalFileHeader, const std::string> >(&handler, &DecompressHandler::onError);
			c.EOk -=Poco::Delegate<DecompressHandler, std::pair<const ZipLocalFileHeader, const Poco::Path> >(&handler, &DecompressHandler::onOk);
		}
		return Application::EXIT_OK;
	}
	
private:
	bool _helpRequested;
	std::string _zipFile;
};


POCO_APP_MAIN(UnzipApp)
