#include <iostream>
#include <iomanip>

#include <boost/assign/list_inserter.hpp>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/NumberParser.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Functions/FunctionsArithmetic.h>
#include <DB/Functions/FunctionsComparison.h>
#include <DB/Functions/FunctionsLogical.h>

#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		typedef std::pair<std::string, SharedPtr<DB::IDataType> > NameAndTypePair;
		typedef std::list<NameAndTypePair> NamesAndTypesList;

		NamesAndTypesList names_and_types_list;

		boost::assign::push_back(names_and_types_list)
			("WatchID",				new DB::DataTypeUInt64)
			("JavaEnable",			new DB::DataTypeUInt8)
			("Title",				new DB::DataTypeString)
			("GoodEvent",			new DB::DataTypeUInt32)
			("EventTime",			new DB::DataTypeDateTime)
			("CounterID",			new DB::DataTypeUInt32)
			("ClientIP",			new DB::DataTypeUInt32)
			("RegionID",			new DB::DataTypeUInt32)
			("UniqID",				new DB::DataTypeUInt64)
			("CounterClass",		new DB::DataTypeUInt8)
			("OS",					new DB::DataTypeUInt8)
			("UserAgent",			new DB::DataTypeUInt8)
			("URL",					new DB::DataTypeString)
			("Referer",				new DB::DataTypeString)
			("Refresh",				new DB::DataTypeUInt8)
			("ResolutionWidth",		new DB::DataTypeUInt16)
			("ResolutionHeight",	new DB::DataTypeUInt16)
			("ResolutionDepth",		new DB::DataTypeUInt8)
			("FlashMajor",			new DB::DataTypeUInt8)
			("FlashMinor",			new DB::DataTypeUInt8)
			("FlashMinor2",			new DB::DataTypeString)
			("NetMajor",			new DB::DataTypeUInt8)
			("NetMinor",			new DB::DataTypeUInt8)
			("UserAgentMajor",		new DB::DataTypeUInt16)
			("UserAgentMinor",		new DB::DataTypeFixedString(2))
			("CookieEnable",		new DB::DataTypeUInt8)
			("JavascriptEnable",	new DB::DataTypeUInt8)
			("IsMobile",			new DB::DataTypeUInt8)
			("MobilePhone",			new DB::DataTypeUInt8)
			("MobilePhoneModel",	new DB::DataTypeString)
			("Params",				new DB::DataTypeString)
			("IPNetworkID",			new DB::DataTypeUInt32)
			("TraficSourceID",		new DB::DataTypeInt8)
			("SearchEngineID",		new DB::DataTypeUInt16)
			("SearchPhrase",		new DB::DataTypeString)
			("AdvEngineID",			new DB::DataTypeUInt8)
			("IsArtifical",			new DB::DataTypeUInt8)
			("WindowClientWidth",	new DB::DataTypeUInt16)
			("WindowClientHeight",	new DB::DataTypeUInt16)
			("ClientTimeZone",		new DB::DataTypeInt16)
			("ClientEventTime",		new DB::DataTypeDateTime)
			("SilverlightVersion1",	new DB::DataTypeUInt8)
			("SilverlightVersion2",	new DB::DataTypeUInt8)
			("SilverlightVersion3",	new DB::DataTypeUInt32)
			("SilverlightVersion4",	new DB::DataTypeUInt16)
			("PageCharset",			new DB::DataTypeString)
			("CodeVersion",			new DB::DataTypeUInt32)
			("IsLink",				new DB::DataTypeUInt8)
			("IsDownload",			new DB::DataTypeUInt8)
			("IsNotBounce",			new DB::DataTypeUInt8)
			("FUniqID",				new DB::DataTypeUInt64)
			("OriginalURL",			new DB::DataTypeString)
			("HID",					new DB::DataTypeUInt32)
			("IsOldCounter",		new DB::DataTypeUInt8)
			("IsEvent",				new DB::DataTypeUInt8)
			("IsParameter",			new DB::DataTypeUInt8)
			("DontCountHits",		new DB::DataTypeUInt8)
			("WithHash",			new DB::DataTypeUInt8)
		;

		SharedPtr<DB::NamesAndTypes> names_and_types_map = new DB::NamesAndTypes;

		for (NamesAndTypesList::const_iterator it = names_and_types_list.begin(); it != names_and_types_list.end(); ++it)
		{
			names_and_types_map->insert(*it);
		}

		DB::Context context;

		(*context.functions)["plus"] 			= new DB::FunctionPlus;
		(*context.functions)["minus"] 			= new DB::FunctionMinus;
		(*context.functions)["multiply"] 		= new DB::FunctionMultiply;
		(*context.functions)["divide"] 			= new DB::FunctionDivideFloating;
		(*context.functions)["intDiv"] 			= new DB::FunctionDivideIntegral;
		(*context.functions)["modulo"] 			= new DB::FunctionModulo;

		(*context.functions)["equals"] 			= new DB::FunctionEquals;
		(*context.functions)["notEquals"] 		= new DB::FunctionNotEquals;
		(*context.functions)["less"] 			= new DB::FunctionLess;
		(*context.functions)["greater"] 		= new DB::FunctionGreater;
		(*context.functions)["lessOrEquals"] 	= new DB::FunctionLessOrEquals;
		(*context.functions)["greaterOrEquals"] = new DB::FunctionGreaterOrEquals;

		(*context.functions)["and"] 			= new DB::FunctionAnd;
		(*context.functions)["or"] 				= new DB::FunctionOr;
		(*context.functions)["xor"] 			= new DB::FunctionXor;
		(*context.functions)["not"] 			= new DB::FunctionNot;

		(*context.databases)["default"]["hits"] 	= new DB::StorageLog("./", "hits", names_and_types_map, ".bin");
		(*context.databases)["system"]["one"] 		= new DB::StorageSystemOne("one");
		(*context.databases)["system"]["numbers"] 	= new DB::StorageSystemNumbers("numbers");
		context.current_database = "default";

		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input;/* =
			"SELECT UniqID, URL, CounterID, IsLink "
			"FROM hits "
			"WHERE URL = 'http://mail.yandex.ru/neo2/#inbox' "
			"LIMIT 10";*/
		std::stringstream str;
		str << std::cin.rdbuf();
		input = str.str();
		
		std::string expected;

		const char * begin = input.data();
		const char * end = begin + input.size();
		const char * pos = begin;

		bool parse_res = parser.parse(pos, end, ast, expected);

		if (!parse_res || pos != end)
			throw DB::Exception("Syntax error: failed at position "
				+ Poco::NumberFormatter::format(pos - begin) + ": "
				+ input.substr(pos - begin, 10)
				+ ", expected " + (parse_res ? "end of data" : expected) + ".",
				DB::ErrorCodes::SYNTAX_ERROR);

		DB::formatAST(*ast, std::cerr);
		std::cerr << std::endl;
/*		std::cerr << ast->getTreeID() << std::endl;
*/
		DB::InterpreterSelectQuery interpreter(ast, context);
		DB::BlockInputStreamPtr in = interpreter.execute();
		
		DB::WriteBufferFromOStream ob(std::cout);
		DB::TabSeparatedRowOutputStream out(ob, new DB::DataTypes(interpreter.getReturnTypes()));

		DB::copyData(*in, out);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
