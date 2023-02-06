//
// Utility.cpp
//
// Library: CppParser
// Package: CppParser
// Module:  Utility
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Utility.h"
#include "Poco/CppParser/Parser.h"
#include "Poco/CppParser/Struct.h"
#include "Poco/StringTokenizer.h"
#include "Poco/Glob.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/Process.h"
#include "Poco/Environment.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Exception.h"
#include <fstream>
#include <cstddef>


using Poco::StringTokenizer;
using Poco::Glob;
using Poco::Path;
using Poco::File;
using Poco::Process;
using Poco::ProcessHandle;
using Poco::Environment;
using Poco::NumberFormatter;
using Poco::Exception;


namespace Poco {
namespace CppParser {


void Utility::parseDir(const std::vector <std::string>& includePattern, const std::vector <std::string>& excludePattern, NameSpace::SymbolTable& st, const std::string& exec, const std::string& options, const std::string& path)
{
	std::set<std::string> files;
	Utility::buildFileList(files, includePattern, excludePattern);
	for (std::set<std::string>::const_iterator it = files.begin(); it != files.end(); ++it)
	{
		Utility::parse(*it, st, exec, options, path);
	}
	Utility::fixup(st);
}


void Utility::parse(const std::string& file, NameSpace::SymbolTable& st, const std::string& exec, const std::string& options, const std::string& path)
{
	std::string prepFile = Utility::preprocessFile(file, exec, options, path);
	Utility::parseOnly(file, st, prepFile, true);
}


void Utility::fixup(NameSpace::SymbolTable& st)
{
	for (NameSpace::SymbolTable::iterator it = st.begin(); it != st.end(); ++it)
	{
		Struct* pStruct = dynamic_cast<Struct*>(it->second);
		if (pStruct)
		{
			pStruct->fixupBases();
		}
	}
}


void Utility::detectPrefixAndIncludes(const std::string& origHFile, std::vector<std::string>& lines, std::string& prefix)
{
	std::ifstream istr(origHFile.c_str());
	try
	{
		if (istr.good())
		{
			std::string x;
			istr >> x;
			while (x.find("#ifndef") == std::string::npos)
				istr >> x;
			StringTokenizer tokenizer(x, " \t", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
			poco_assert (tokenizer.count() == 2);
			StringTokenizer::Iterator itTmp = tokenizer.begin();
			++itTmp;
			std::string defValue = *itTmp;
			istr >> x;
			// now find the corresponsing #define
			while (x.find(defValue) == std::string::npos)
				istr >> x;
			 //now parse until a class def is found without a ; at the end
			bool stop = false;
			std::string prefixHint;
			do
			{
				istr >> x;
				// we add EVERYTHING to lines: reason: used macros/preprocessor defines, conditional includes should all be present in the generated code
				// just think about fwd declarations inside a NS_BEGIN ... NS_END block
				if (x.find("class") != std::string::npos && x.find(";") == std::string::npos)
				{
					StringTokenizer tok(x, " \t", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
					StringTokenizer::Iterator it = tok.begin();
					while (*it != "class" && it != tok.end())
						++it;
					// the next after class must be *_API or in case of a template it must be the class name
					++it;
					std::size_t apiPos = it->find("_API");
					if (apiPos != std::string::npos)
					{
						prefixHint = it->substr(0, apiPos);
					}
					stop = true;
				}
				else
				{
					lines.push_back(x);
				}
			}
			while (!stop && !istr.eof());
			if (!stop)
			{
				lines.clear();
				prefix.clear();
				//throw Poco::SyntaxException("Failed to parse file " + origHFile + ".No class declared?");
				return;
			}

			// now search the prefix
			if (lines.empty())
			{
				prefix.clear();
				return;
			}
			// the prefix for that file
			std::vector<std::string>::const_iterator it = lines.end();
			--it;
			std::vector<std::string>::const_iterator itBegin = lines.begin();
			for (; it != itBegin; --it)
			{
				std::size_t prefixPos = it->find("_BEGIN");
				if (prefixPos != std::string::npos)
				{
					prefix = it->substr(0, prefixPos);
					if (prefix != prefixHint && !prefixHint.empty())
					{
						throw Poco::SyntaxException("Conflicting prefixes detected: " + prefixHint + "<->" + prefix);
					}
				}
			}
		}
		else throw Poco::OpenFileException(origHFile);
	}
	catch (Exception&)
	{
		istr.close();
		throw;
	}
	istr.close();
}


std::string Utility::preprocessFile(const std::string& file, const std::string& exec, const std::string& options, const std::string& path)
{
	Path pp(file);
	pp.setExtension("i");

	std::string popts;
	for (std::string::const_iterator it = options.begin(); it != options.end(); ++it)
	{
		if (*it == '%')
			popts += pp.getBaseName();
		else
			popts += *it;
	}
	StringTokenizer tokenizer(popts, ",;\n", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
	std::vector<std::string> args(tokenizer.begin(), tokenizer.end());
#ifdef _WIN32
	std::string quotedFile("\"");
	quotedFile += file;
	quotedFile += "\"";
	args.push_back(quotedFile);
#else
	args.push_back(file);
#endif
	if (!path.empty())
	{
		std::string newPath(Environment::get("PATH"));
		newPath += Path::pathSeparator();
		newPath += path;
		Environment::set("PATH", path);
	}
	
	ProcessHandle proc = Process::launch(exec, args);		
	int rc = Process::wait(proc);
	if (rc != 0)
	{
		throw Poco::RuntimeException("Failed to process file");
	}
	
	return pp.getFileName();
}


void Utility::parseOnly(const std::string& file, NameSpace::SymbolTable& st, const std::string& preprocessedFile, bool removePreprocessedFile)
{
	std::ifstream istr(preprocessedFile.c_str());
	try
	{
		if (istr.good())
		{
			Parser parser(st, file, istr);
			parser.parse();
		}
		else throw Poco::OpenFileException(preprocessedFile);
	}
	catch (Exception&)
	{
		istr.close();
		if (removePreprocessedFile)
			removeFile(preprocessedFile);
		throw;
	}
	istr.close();
	if (removePreprocessedFile)
		removeFile(preprocessedFile);
}


void Utility::removeFile(const std::string& preprocessedfile)
{
	try
	{
		File f(preprocessedfile);
		f.remove();
	}
	catch (Exception&)
	{
	}
}


void Utility::buildFileList(std::set<std::string>& files, const std::vector<std::string>& includePattern, const std::vector<std::string>& excludePattern)
{
	std::set<std::string> temp;
	std::vector <std::string>::const_iterator itInc = includePattern.begin();
	std::vector <std::string>::const_iterator itIncEnd = includePattern.end();

	int options(0);
#if defined(_WIN32)
	options |= Glob::GLOB_CASELESS;
#endif

	for (; itInc != itIncEnd; ++itInc)
	{
		Glob::glob(*itInc, temp, options);
	}

	for (std::set<std::string>::const_iterator it = temp.begin(); it != temp.end(); ++it)
	{
		Path p(*it);
		bool include = true;
		std::vector <std::string>::const_iterator itExc = excludePattern.begin();
		std::vector <std::string>::const_iterator itExcEnd = excludePattern.end();
		for (; itExc != itExcEnd; ++itExc)
		{
			Glob glob(*itExc, options);
			if (glob.match(p.toString()))
				include = false;
		}
		if (include)
			files.insert(*it);
	}
}


std::string replace(const std::string& input, const std::string& oldToken, const std::string& newToken)
{
	std::string result;
	std::size_t start = 0;
	std::size_t pos = 0;
	do
	{
		pos = input.find(oldToken, start);
		result.append(input.substr(start, pos-start));
		if (pos != std::string::npos)
			result.append(newToken);
		start = pos + oldToken.length();
	}
	while (pos != std::string::npos);
	
	return result;
}

} } // namespace Poco::CppParser
