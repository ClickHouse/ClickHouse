//
// TextEncodingCompiler.cpp
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Application.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include "Poco/Util/MapConfiguration.h"
#include "Poco/URI.h"
#include "Poco/URIStreamOpener.h"
#include "Poco/StringTokenizer.h"
#include "Poco/NumberParser.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Exception.h"
#include "Poco/DateTime.h"
#include "Poco/AutoPtr.h"
#include "Poco/Path.h"
#include "Poco/Net/HTTPStreamFactory.h"
#include "Poco/Net/FTPStreamFactory.h"
#include <iostream>
#include <fstream>
#include <memory>
#include <map>


using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;
using Poco::Util::AbstractConfiguration;
using Poco::Util::OptionCallback;
using Poco::Net::HTTPStreamFactory;
using Poco::Net::FTPStreamFactory;
using Poco::URIStreamOpener;
using Poco::StringTokenizer;
using Poco::NumberParser;
using Poco::NumberFormatter;


class TextEncodingCompiler: public Application
{
public:
	TextEncodingCompiler():
		_helpRequested(false),
		_pVars(new Poco::Util::MapConfiguration)
	{
		for (int i = 0; i < 256; i++)
		{
			_characterMap[i] = -1;
		}
	}

protected:
	void initialize(Application& self)
	{
		loadConfiguration();
		Application::initialize(self);

		HTTPStreamFactory::registerFactory();
		FTPStreamFactory::registerFactory();
	}

	void defineOptions(OptionSet& options)
	{
		Application::defineOptions(options);

		options.addOption(
			Option("help", "h", "Display help information on command line arguments.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<TextEncodingCompiler>(this, &TextEncodingCompiler::handleHelp)));

		options.addOption(
			Option("class-name", "c", "Specify the encoding class name.")
				.required(true)
				.repeatable(false)
				.argument("className")
				.callback(OptionCallback<TextEncodingCompiler>(this, &TextEncodingCompiler::handleClassName)));

		options.addOption(
			Option("encoding-name", "e", "Specify the encoding name. Can be specified multiple times.")
				.required(true)
				.repeatable(true)
				.argument("encodingName")
				.callback(OptionCallback<TextEncodingCompiler>(this, &TextEncodingCompiler::handleEncodingName)));
	}

	void handleHelp(const std::string& name, const std::string& value)
	{
		_helpRequested = true;
		displayHelp();
		stopOptionsProcessing();
	}

	void handleClassName(const std::string& name, const std::string& value)
	{
		_className = value;
	}

	void handleEncodingName(const std::string& name, const std::string& value)
	{
		_encodingNames.push_back(value);
	}

	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("{options} <table-URI>");
		helpFormatter.setHeader(
			"\n"
			"The POCO C++ Text Encodings Compiler.\n"
			"Copyright (c) 2018 by Applied Informatics Software Engineering GmbH.\n"
			"All rights reserved.\n\n"
			"This program compiles Unicode character encoding tables "
			"from http://www.unicode.org/Public/MAPPINGS/ to TextEncoding "
			"classes for the Poco Encodings library. \n\n"
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

	void readMapping(std::istream& istr)
	{
		std::string line;
		while (std::getline(istr, line))
		{
			std::string::size_type cpos = line.find('#');
			if (cpos != std::string::npos) line.resize(cpos);
			StringTokenizer tok(line, "\t", Poco::StringTokenizer::TOK_IGNORE_EMPTY | Poco::StringTokenizer::TOK_TRIM);
			if (tok.count() == 2)
			{
				unsigned from;
				unsigned to;
				if (NumberParser::tryParseHex(tok[0], from) && NumberParser::tryParseHex(tok[1], to))
				{
					_mapping[from] = to;
					if (from <= 0xFF)
						_characterMap[from] = to;
					else
						_characterMap[from >> 8] = -2;
				}
			}
		}
	}

	void reverseMapping()
	{
		for (std::map<int, int>::const_iterator it = _mapping.begin(); it != _mapping.end(); ++it)
		{
			_reverseMapping[it->second] = it->first;
		}
	}

	void defineEncodingNames()
	{
		std::string names;
		for (std::vector<std::string>::const_iterator it = _encodingNames.begin(); it != _encodingNames.end(); ++it)
		{
			names += "\t\"";
			names += *it;
			names += "\",\n";
		}
		_pVars->setString("ENCODING_NAMES", names);
	}

	void defineCharacterMap()
	{
		std::string charMap;
		for (int i = 0; i < 256; i++)
		{
			if (i % 16 == 0 && i > 0) charMap += "\n";
			if (i % 16 == 0) charMap += "\t";
			if (_characterMap[i] >= 0)
			{
				charMap += "0x";
				charMap += NumberFormatter::formatHex(_characterMap[i], 4);
			}
			else
			{
				charMap += NumberFormatter::format(_characterMap[i], 6);
			}
			charMap += ", ";
		}
		_pVars->setString("CHARACTER_MAP", charMap);
	}

	void defineEncodingTable(const std::map<int, int>& map, const std::string& name, int start)
	{
		std::string table = "\t";
		int n = 0;
		for (std::map<int, int>::const_iterator it = map.begin(); it != map.end(); ++it)
		{
			if (it->first >= start)
			{
				if (n % 8 == 0 && n > 0) table += "\n\t";
				table += "{ 0x";
				table += NumberFormatter::formatHex(it->first, 4);
				table += ", 0x";
				table += NumberFormatter::formatHex(it->second, 4);
				table += " }, ";
				n++;
			}
		}
		if (table == "\t") table = "\t{ 0x0000, 0x0000 } // dummy entry";
		_pVars->setString(name, table);
	}

	void defineVars(const std::string& source)
	{
		_pVars->setString("SOURCE", source);
		_pVars->setString("CLASS", _className);
		_pVars->setString("ENCODING", _encodingNames[0]);
		_pVars->setInt("YEAR", Poco::DateTime().year());
		defineEncodingNames();
		defineCharacterMap();
		defineEncodingTable(_mapping, "MAPPING_TABLE", 256);
		defineEncodingTable(_reverseMapping, "REVERSE_MAPPING_TABLE", 0);
	}

	int main(const ArgVec& args)
	{
		if (!_helpRequested && args.size() > 0)
		{
			Poco::URI encodingURI(args[0]);
			std::unique_ptr<std::istream> pEncodingStream(URIStreamOpener::defaultOpener().open(encodingURI));
			readMapping(*pEncodingStream);
			reverseMapping();
			defineVars(encodingURI.toString());

			std::string headerPath("include/Poco/");
			headerPath += _className;
			headerPath += ".h";
			std::ofstream headerStream(headerPath.c_str());
			if (headerStream.good())
				headerStream << _pVars->expand(HEADER_TEMPLATE);
			if (!headerStream.good()) throw Poco::CreateFileException(headerPath);
			headerStream.close();

			std::string implPath("src/");
			implPath += _className;
			implPath += ".cpp";
			std::ofstream implStream(implPath.c_str());
			if (implStream.good())
				implStream << _pVars->expand(IMPL_TEMPLATE);
			if (!implStream.good()) throw Poco::CreateFileException(implPath);
			implStream.close();
		}
		return Application::EXIT_OK;
	}

private:
	bool _helpRequested;
	int _characterMap[256];
	std::map<int, int> _mapping;
	std::map<int, int> _reverseMapping;
	std::string _className;
	std::vector<std::string> _encodingNames;
	Poco::AutoPtr<Poco::Util::MapConfiguration> _pVars;
	static const std::string HEADER_TEMPLATE;
	static const std::string IMPL_TEMPLATE;
};


POCO_APP_MAIN(TextEncodingCompiler)


const std::string TextEncodingCompiler::HEADER_TEMPLATE(
	"//\n"
	"// ${CLASS}.h\n"
	"//\n"
	"// Library: Encodings\n"
	"// Package: Encodings\n"
	"// Module:  ${CLASS}\n"
	"//\n"
	"// Definition of the Windows1252Encoding class.\n"
	"//\n"
	"// Copyright (c) ${YEAR}, Applied Informatics Software Engineering GmbH.\n"
	"// and Contributors.\n"
	"//\n"
	"// SPDX-License-Identifier: BSL-1.0\n"
	"//\n"
	"\n"
	"\n"
	"#ifndef Encodings_${CLASS}_INCLUDED\n"
	"#define Encodings_${CLASS}_INCLUDED\n"
	"\n"
	"\n"
	"#include \"Poco/DoubleByteEncoding.h\"\n"
	"\n"
	"\n"
	"namespace Poco {\n"
	"\n"
	"\n"
	"class Encodings_API ${CLASS}: public DoubleByteEncoding\n"
	"\t/// ${ENCODING} Encoding.\n"
	"\t///\n"
	"\t/// This text encoding class has been generated from\n"
	"\t/// ${SOURCE}.\n"
	"{\n"
	"public:\n"
	"\t${CLASS}();\n"
	"\t~${CLASS}();\n"
	"\t\n"
	"private:\n"
	"\tstatic const char* _names[];\n"
	"\tstatic const CharacterMap _charMap;\n"
	"\tstatic const Mapping _mappingTable[];\n"
	"\tstatic const Mapping _reverseMappingTable[];\n"
	"};\n"
	"\n"
	"\n"
	"} // namespace Poco\n"
	"\n"
	"\n"
	"#endif // Encodings_${CLASS}_INCLUDED\n"
);


const std::string TextEncodingCompiler::IMPL_TEMPLATE(
	"//\n"
	"// ${CLASS}.cpp\n"
	"//\n"
	"// Library: Encodings\n"
	"// Package: Encodings\n"
	"// Module:  ${CLASS}\n"
	"//\n"
	"// Copyright (c) ${YEAR}, Applied Informatics Software Engineering GmbH.\n"
	"// and Contributors.\n"
	"//\n"
	"// SPDX-License-Identifier: BSL-1.0\n"
	"//\n"
	"\n"
	"\n"
	"#include \"Poco/${CLASS}.h\"\n"
	"\n"
	"\n"
	"namespace Poco {\n"
	"\n"
	"\n"
	"const char* ${CLASS}::_names[] =\n"
	"{\n"
	"${ENCODING_NAMES}"
	"	NULL\n"
	"};\n"
	"\n"
	"\n"
	"const TextEncoding::CharacterMap ${CLASS}::_charMap = \n"
	"{\n"
	"${CHARACTER_MAP}\n"
	"};\n"
	"\n"
	"\n"
	"const DoubleByteEncoding::Mapping ${CLASS}::_mappingTable[] = {\n"
	"${MAPPING_TABLE}\n"
	"};\n"
	"\n"
	"\n"
	"const DoubleByteEncoding::Mapping ${CLASS}::_reverseMappingTable[] = {\n"
	"${REVERSE_MAPPING_TABLE}\n"
	"};\n"
	"\n"
	"\n"
	"${CLASS}::${CLASS}():\n"
	"	DoubleByteEncoding(_names, _charMap, _mappingTable, sizeof(_mappingTable)/sizeof(Mapping), _reverseMappingTable, sizeof(_reverseMappingTable)/sizeof(Mapping))\n"
	"{\n"
	"}\n"
	"\n"
	"\n"
	"${CLASS}::~${CLASS}()\n"
	"{\n"
	"}\n"
	"\n"
	"\n"
	"} // namespace Poco\n"
);

