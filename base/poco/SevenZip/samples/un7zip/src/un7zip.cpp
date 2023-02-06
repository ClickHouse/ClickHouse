//
// un7zip.cpp
//
// This sample demonstrates the Archive class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SevenZip/Archive.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Delegate.h"
#include <iostream>
#include <iomanip>


using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;
using Poco::Util::AbstractConfiguration;
using Poco::Util::OptionCallback;


class Un7zipApp: public Application
{
public:
	Un7zipApp(): 
		_helpRequested(false),
		_list(false),
		_extract(false)
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
			Option("help", "h", "Display help information on command line arguments.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<Un7zipApp>(this, &Un7zipApp::handleHelp)));

		options.addOption(
			Option("output", "o", "Specify base directory for extracted files.")
				.required(false)
				.repeatable(false)
				.argument("path")
				.callback(OptionCallback<Un7zipApp>(this, &Un7zipApp::handleOutput)));
				
		options.addOption(
			Option("list", "l", "List files and directories in archive.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<Un7zipApp>(this, &Un7zipApp::handleList)));

		options.addOption(
			Option("extract", "x", "Extract single file or entire archive.")
				.required(false)
				.repeatable(false)
				.argument("file", false)
				.callback(OptionCallback<Un7zipApp>(this, &Un7zipApp::handleExtract)));
	}
	
	void handleHelp(const std::string& name, const std::string& value)
	{
		_helpRequested = true;
		displayHelp();
		stopOptionsProcessing();
	}

	void handleOutput(const std::string& name, const std::string& value)
	{
		_outputPath = value;
	}
	
	void handleList(const std::string& name, const std::string& value)
	{
		_list = true;
	}
	
	void handleExtract(const std::string& name, const std::string& value)
	{
		_extract = true;
		_extractFile = value;
	}
	
	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("OPTIONS archive");
		helpFormatter.setHeader("A application that demonstrates usage of Poco::SevenZip::Archive.");
		helpFormatter.format(std::cout);
	}
	
	void onExtracted(const Poco::SevenZip::Archive::ExtractedEventArgs& args)
	{
		std::cout << "Extracted: " << args.entry.path() << " ===> " << args.extractedPath << std::endl;
	}
	
	void onFailed(const Poco::SevenZip::Archive::FailedEventArgs& args)
	{
		std::cout << "Failed: " << args.entry.path() << ": " << args.pException->displayText() << std::endl;
	}

	int main(const std::vector<std::string>& args)
	{
		if (!_helpRequested && !args.empty())
		{
			Poco::SevenZip::Archive archive(args[0]);
			
			if (_list)
			{
				for (Poco::SevenZip::Archive::ConstIterator it = archive.begin(); it != archive.end(); ++it)
				{
					std::cout 
						<< (it->isFile() ? '-' : 'd') 
						<< " "
						<< Poco::DateTimeFormatter::format(it->lastModified(), Poco::DateTimeFormat::SORTABLE_FORMAT)
						<< " " 
						<< std::setw(12) << it->size()
						<< " " 
						<< it->path()
						<< std::endl;
				}
			}
			
			if (_extract)
			{			
				archive.extracted += Poco::delegate(this, &Un7zipApp::onExtracted);
				archive.failed += Poco::delegate(this, &Un7zipApp::onFailed);
			
				if (_extractFile.empty())
				{
					archive.extract(_outputPath);
				}
				else
				{
					for (Poco::SevenZip::Archive::ConstIterator it = archive.begin(); it != archive.end(); ++it)
					{
						if (it->path() == _extractFile)
						{
							archive.extract(*it, _outputPath);
						}
					}
				}
			}
		}
		return Application::EXIT_OK;
	}
	
private:
	bool _helpRequested;
	std::string _outputPath;
	std::string _extractFile;
	bool _list;
	bool _extract;
};


POCO_APP_MAIN(Un7zipApp)
