//
// Benchmark.cpp
//
// $Id$
//
// This sample shows a benchmark of the JSON parser.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Parser.h"
#include "Poco/JSON/ParseHandler.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/Environment.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/Stopwatch.h"
#include <iostream>
#include <iomanip>


int main(int argc, char** argv)
{
	Poco::Stopwatch sw;

	std::string dir = Poco::Environment::get("POCO_BASE") + "/JSON/samples/Benchmark/";
	Poco::Path filePath(dir, "input.big.json");

	std::ostringstream ostr;

	if (filePath.isFile())
	{
		Poco::File inputFile(filePath);
		if ( inputFile.exists() )
		{
			sw.start();
			Poco::FileInputStream fis(filePath.toString());
			Poco::StreamCopier::copyStream(fis, ostr);
			sw.stop();
		}
		else
		{
			std::cout << filePath.toString() << " doesn't exist!" << std::endl;
			return 1; 
		}
	}

	std::cout << "JSON Benchmark" << std::endl;
	std::cout << "==============" << std::endl;

	std::string jsonStr = ostr.str();
	std::cout << "Total of " << jsonStr.size() << " bytes," << std::endl << "loaded in " << sw.elapsed() << " [us]," << std::endl;

	std::cout << std::endl << "POCO JSON barebone parse" << std::endl;
	Poco::JSON::Parser sparser(0);
	sw.restart();
	sparser.parse(jsonStr);
	sw.stop();
	std::cout << "---------------------------------" << std::endl;
	std::cout << "[std::string] parsed in " << sw.elapsed() << " [us]" << std::endl;
	std::cout << "---------------------------------" << std::endl;

	Poco::JSON::Parser iparser(0);
	std::istringstream istr(jsonStr);
	sw.restart();
	iparser.parse(istr);
	sw.stop();
	std::cout << "----------------------------------------" << std::endl;
	std::cout << "[std::istringstream] parsed in " << sw.elapsed() << " [us]" << std::endl;
	std::cout << "----------------------------------------" << std::endl;

	std::cout << std::endl << "POCO JSON Handle/Stringify" << std::endl;
	try
	{
		Poco::JSON::Parser sparser;
		sw.restart();
		sparser.parse(jsonStr);
		Poco::DynamicAny result = sparser.result();
		sw.stop();
		std::cout << "-----------------------------------------" << std::endl;
		std::cout << "[std::string] parsed/handled in " << sw.elapsed() << " [us]" << std::endl;
		std::cout << "-----------------------------------------" << std::endl;

		Poco::JSON::Parser isparser;
		std::istringstream istr(jsonStr);
		sw.restart();
		isparser.parse(istr);
		result = isparser.result();
		sw.stop();
		std::cout << "------------------------------------------------" << std::endl;
		std::cout << "[std::istringstream] parsed/handled in " << sw.elapsed() << " [us]" << std::endl;
		std::cout << "------------------------------------------------" << std::endl;

		//Serialize to string
		Poco::JSON::Object::Ptr obj;
		if ( result.type() == typeid(Poco::JSON::Object::Ptr) )
			obj = result.extract<Poco::JSON::Object::Ptr>();

		std::ostringstream out;
		sw.restart();
		obj->stringify(out);
		sw.stop();
		std::cout << "-----------------------------------" << std::endl;
		std::cout << "stringified in " << sw.elapsed() << " [us]" << std::endl;
		std::cout << "-----------------------------------" << std::endl;
		std::cout << std::endl;
	}
	catch(Poco::JSON::JSONException jsone)
	{
		std::cout << jsone.message() << std::endl;
	}

	return 0;
}
