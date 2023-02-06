//
// PageCompiler.cpp
//
// A compiler that compiler HTML pages containing JSP directives into C++ classes.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
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
#include "Poco/FileStream.h"
#include "Poco/Path.h"
#include "Poco/DateTime.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/StringTokenizer.h"
#include "Poco/LineEndingConverter.h"
#include "Poco/Ascii.h"
#include "Page.h"
#include "PageReader.h"
#include "CodeWriter.h"
#include "ApacheCodeWriter.h"
#include "OSPCodeWriter.h"
#include <sstream>
#include <iostream>
#include <memory>


using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;
using Poco::Util::AbstractConfiguration;
using Poco::Util::OptionCallback;
using Poco::AutoPtr;
using Poco::FileInputStream;
using Poco::FileOutputStream;
using Poco::Path;
using Poco::DateTime;
using Poco::DateTimeFormatter;
using Poco::DateTimeFormat;
using Poco::StringTokenizer;
using Poco::OutputLineEndingConverter;


class CompilerApp: public Application
{
public:
	CompilerApp():
		_helpRequested(false),
		_generateOSPCode(false),
		_generateApacheCode(false),
		_emitLineDirectives(true)
	{
	}

protected:
	void initialize(Application& self)
	{
		loadConfiguration(); // load default configuration files, if present
		Application::initialize(self);
	}

	void defineOptions(OptionSet& options)
	{
		Application::defineOptions(options);

		options.addOption(
			Option("help", "h", "Display help information on command line arguments.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleHelp)));

		options.addOption(
			Option("define", "D",
				"Define a configuration property. A configuration property "
				"defined with this option can be referenced in the input "
				"page file, using the following syntax: ${<name>}.")
				.required(false)
				.repeatable(true)
				.argument("<name>=<value>")
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleDefine)));

		options.addOption(
			Option("config-file", "f", "Load configuration data from the given file.")
				.required(false)
				.repeatable(true)
				.argument("<file>")
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleConfig)));

		options.addOption(
			Option("output-dir", "o", "Write output files to directory <dir>.")
				.required(false)
				.repeatable(false)
				.argument("<dir>")
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleOutputDir)));

		options.addOption(
			Option("header-output-dir", "H", "Write header file to directory <dir>.")
				.required(false)
				.repeatable(false)
				.argument("<dir>")
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleHeaderOutputDir)));

		options.addOption(
			Option("header-prefix", "P", "Prepend the given <prefix> to the header file name in the generated #include directive.")
				.required(false)
				.repeatable(false)
				.argument("<prefix>")
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleHeaderPrefix)));

		options.addOption(
			Option("base-file-name", "b", "Use <name> instead of the class name for the output file name.")
				.required(false)
				.repeatable(false)
				.argument("<name>")
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleBase)));

		options.addOption(
			Option("osp", "O", "Add factory class definition and implementation for use with the Open Service Platform.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleOSP)));

		options.addOption(
			Option("apache", "A", "Add factory class definition and implementation, and shared library manifest for use with ApacheConnector.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleApache)));

		options.addOption(
			Option("noline", "N", "Do not include #line directives in generated code.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<CompilerApp>(this, &CompilerApp::handleNoLine)));
	}

	void handleHelp(const std::string& name, const std::string& value)
	{
		_helpRequested = true;
		stopOptionsProcessing();
	}

	void handleDefine(const std::string& name, const std::string& value)
	{
		defineProperty(value);
	}

	void handleConfig(const std::string& name, const std::string& value)
	{
		loadConfiguration(value);
	}

	void handleOutputDir(const std::string& name, const std::string& value)
	{
		_outputDir = value;
	}

	void handleHeaderOutputDir(const std::string& name, const std::string& value)
	{
		_headerOutputDir = value;
	}

	void handleHeaderPrefix(const std::string& name, const std::string& value)
	{
		_headerPrefix = value;
		if (!_headerPrefix.empty() && _headerPrefix[_headerPrefix.size() - 1] != '/')
			_headerPrefix += '/';
	}

	void handleBase(const std::string& name, const std::string& value)
	{
		_base = value;
	}

	void handleOSP(const std::string& name, const std::string& value)
	{
		_generateOSPCode = true;
	}

	void handleApache(const std::string& name, const std::string& value)
	{
		_generateApacheCode = true;
	}

	void handleNoLine(const std::string& name, const std::string& value)
	{
		_emitLineDirectives = false;
	}

	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("[<option> ...] <file> ...");
		helpFormatter.setHeader(
			"\n"
			"The POCO C++ Server Page Compiler.\n"
			"Copyright (c) 2008-2019 by Applied Informatics Software Engineering GmbH.\n"
			"All rights reserved.\n\n"
			"This program compiles web pages containing embedded C++ code "
			"into a C++ class that can be used with the HTTP server "
			"from the POCO Net library. \n\n"
			"The following command line options are supported:"
		);
		helpFormatter.setFooter(
			"For more information, please see the POCO C++ Libraries "
			"documentation at <http://pocoproject.org/docs/>."
		);
		helpFormatter.setIndent(8);
		helpFormatter.format(std::cout);
	}

	void defineProperty(const std::string& def)
	{
		std::string name;
		std::string value;
		std::string::size_type pos = def.find('=');
		if (pos != std::string::npos)
		{
			name.assign(def, 0, pos);
			value.assign(def, pos + 1, def.length() - pos);
		}
		else name = def;
		config().setString(name, value);
	}

	int main(const std::vector<std::string>& args)
	{
		if (_helpRequested || args.empty())
		{
			displayHelp();
			return Application::EXIT_OK;
		}

		for (std::vector<std::string>::const_iterator it = args.begin(); it != args.end(); ++it)
		{
			compile(*it);
		}

		return Application::EXIT_OK;
	}

	void parse(const std::string& path, Page& page, std::string& clazz)
	{
		FileInputStream srcStream(path);
		PageReader pageReader(page, path);
		pageReader.emitLineDirectives(_emitLineDirectives);
		pageReader.parse(srcStream);

		Path p(path);

		if (page.has("page.class"))
		{
			clazz = page.get("page.class");
		}
		else
		{
			clazz = p.getBaseName() + "Handler";
			clazz[0] = Poco::Ascii::toUpper(clazz[0]);
		}
	}

	void write(const std::string& path, const Page& page, const std::string& clazz)
	{
		Path p(path);
		config().setString("inputFileName", p.getFileName());
		config().setString("inputFilePath", p.toString());

		DateTime now;
		config().setString("dateTime", DateTimeFormatter::format(now, DateTimeFormat::SORTABLE_FORMAT));

		if (page.has("page.class"))
		{
			p.setBaseName(clazz);
		}

#ifndef POCO_ENABLE_CPP11
		std::auto_ptr<CodeWriter> pCodeWriter(createCodeWriter(page, clazz));
#else
		std::unique_ptr<CodeWriter> pCodeWriter(createCodeWriter(page, clazz));
#endif

		if (!_outputDir.empty())
		{
			p = Path(_outputDir, p.getBaseName());
		}

		if (!_base.empty())
		{
			p.setBaseName(_base);
		}

		p.setExtension("cpp");
		std::string implPath = p.toString();
		std::string implFileName = p.getFileName();

		if (!_headerOutputDir.empty())
		{
			p = Path(_headerOutputDir, p.getBaseName());
		}
		p.setExtension("h");
		std::string headerPath = p.toString();
		std::string headerFileName = p.getFileName();

		config().setString("outputFileName", implFileName);
		config().setString("outputFilePath", implPath);
		FileOutputStream implStream(implPath);
		OutputLineEndingConverter implLEC(implStream);
		writeFileHeader(implLEC);
		pCodeWriter->writeImpl(implLEC, _headerPrefix + headerFileName);

		config().setString("outputFileName", headerFileName);
		config().setString("outputFilePath", headerPath);
		FileOutputStream headerStream(headerPath);
		OutputLineEndingConverter headerLEC(headerStream);
		writeFileHeader(headerLEC);
		pCodeWriter->writeHeader(headerLEC, headerFileName);
	}

	void compile(const std::string& path)
	{
		Page page;
		std::string clazz;
		parse(path, page, clazz);
		write(path, page, clazz);

		FileInputStream srcStream(path);
		PageReader pageReader(page, path);
		pageReader.emitLineDirectives(_emitLineDirectives);
		pageReader.parse(srcStream);

	}

	void writeFileHeader(std::ostream& ostr)
	{
		std::string fileHeader = config().getString("PageCompiler.fileHeader", "");
		if (!fileHeader.empty())
		{
			ostr << fileHeader << std::endl;
			ostr << "\n\n";
		}
	}

	CodeWriter* createCodeWriter(const Page& page, const std::string& clazz)
	{
		if (_generateOSPCode)
			return new OSPCodeWriter(page, clazz);
		else if (_generateApacheCode)
			return new ApacheCodeWriter(page, clazz);
		else
			return new CodeWriter(page, clazz);
	}

private:
	bool _helpRequested;
	bool _generateOSPCode;
	bool _generateApacheCode;
	bool _emitLineDirectives;
	std::string _outputDir;
	std::string _headerOutputDir;
	std::string _headerPrefix;
	std::string _base;
};


POCO_APP_MAIN(CompilerApp)
