//
// zip.cpp
//
// $Id: //poco/1.4/Zip/samples/zip/src/zip.cpp#1 $
//
// This sample demonstrates the Compress class.
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
#include "Poco/Zip/Compress.h"
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
using Poco::AutoPtr;


class ZipApp: public Application
	/// This sample demonstrates some of the features of the Util::Application class,
	/// such as configuration file handling and command line arguments processing.
	///
	/// Try zip --help (on Unix platforms) or zip /help (elsewhere) for
	/// more information.
{
public:
	ZipApp(): _helpRequested(false)
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
				.callback(OptionCallback<ZipApp>(this, &ZipApp::handleHelp)));

		options.addOption(
			Option("file", "f", "specifies the output zip file")
				.required(true)
				.repeatable(false)
				.argument("filename")
				.callback(OptionCallback<ZipApp>(this, &ZipApp::handleFile)));
	}
	
	void handleHelp(const std::string& name, const std::string& value)
	{
		_helpRequested = true;
		displayHelp();
		stopOptionsProcessing();
	}

	void handleFile(const std::string& name, const std::string& value)
	{
		_outFile = value;
	}
	
	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("OPTIONS <set of input files>");
		helpFormatter.setHeader("A application that demonstrates usage of Poco::Zip::Compress class.");
		helpFormatter.format(std::cout);
	}
	
	void onDone(const void*, const Poco::Zip::ZipLocalFileHeader& hdr)
	{
		logger().information("DONE: " + hdr.getFileName());
	}

	int main(const std::vector<std::string>& args)
	{
		if (!_helpRequested)
		{
			if (args.empty())
			{
				logger().warning("No input files specified for compression");
			}
			else
			{
				std::ofstream out(_outFile.c_str(), std::ios::binary);
				Poco::Zip::Compress c(out, true);
				c.EDone += Poco::Delegate<ZipApp, const Poco::Zip::ZipLocalFileHeader>(this, &ZipApp::onDone);
				for (std::vector<std::string>::const_iterator it = args.begin(); it != args.end(); ++it)
				{
					// no WILDCARD support!
					Poco::File aFile(*it);
					if (!aFile.exists())
					{
						logger().error("File not found: " + *it);
					}
					else
					{
						Poco::Path anEntry(*it);
						if (aFile.isDirectory())
						{
							anEntry.makeDirectory();
							c.addRecursive(anEntry, Poco::Zip::ZipCommon::CL_MAXIMUM, (args.size() != 1)); // add root if we have more than one entry
						}
						else
						{
							anEntry.makeFile();
							c.addFile(anEntry, anEntry);
						}
					}
				}
				c.close();
				c.EDone -= Poco::Delegate<ZipApp, const Poco::Zip::ZipLocalFileHeader>(this, &ZipApp::onDone);
			}
		}
		return Application::EXIT_OK;
	}
	
private:
	bool _helpRequested;
	std::string _outFile;
};


POCO_APP_MAIN(ZipApp)
