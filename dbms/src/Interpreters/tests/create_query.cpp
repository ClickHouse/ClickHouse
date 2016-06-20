#include <iostream>
#include <iomanip>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Databases/DatabaseOrdinary.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		std::string input = "CREATE TABLE IF NOT EXISTS hits (\n"
			"WatchID				UInt64,\n"
			"JavaEnable 			UInt8,\n"
			"Title 					String,\n"
			"EventTime 				DateTime,\n"
			"CounterID 				UInt32,\n"
			"ClientIP 				UInt32,\n"
			"RegionID 				UInt32,\n"
			"UniqID 				UInt64,\n"
			"CounterClass 			UInt8,\n"
			"OS 					UInt8,\n"
			"UserAgent 				UInt8,\n"
			"URL 					String,\n"
			"Referer 				String,\n"
			"ResolutionWidth 		UInt16,\n"
			"ResolutionHeight 		UInt16,\n"
			"ResolutionDepth 		UInt8,\n"
			"FlashMajor 			UInt8,\n"
			"FlashMinor 			UInt8,\n"
			"FlashMinor2 			String,\n"
			"NetMajor 				UInt8,\n"
			"NetMinor 				UInt8,\n"
			"UserAgentMajor 		UInt16,\n"
			"UserAgentMinor 		FixedString(2),\n"
			"CookieEnable 			UInt8,\n"
			"JavascriptEnable 		UInt8,\n"
			"IsMobile 				UInt8,\n"
			"MobilePhone 			UInt8,\n"
			"MobilePhoneModel 		String,\n"
			"Params 				String,\n"
			"IPNetworkID 			UInt32,\n"
			"TraficSourceID 		Int8,\n"
			"SearchEngineID 		UInt16,\n"
			"SearchPhrase 			String,\n"
			"AdvEngineID 			UInt8,\n"
			"IsArtifical 			UInt8,\n"
			"WindowClientWidth 		UInt16,\n"
			"WindowClientHeight 	UInt16,\n"
			"ClientTimeZone 		Int16,\n"
			"ClientEventTime 		DateTime,\n"
			"SilverlightVersion1 	UInt8,\n"
			"SilverlightVersion2 	UInt8,\n"
			"SilverlightVersion3 	UInt32,\n"
			"SilverlightVersion4 	UInt16,\n"
			"PageCharset 			String,\n"
			"CodeVersion 			UInt32,\n"
			"IsLink 				UInt8,\n"
			"IsDownload 			UInt8,\n"
			"IsNotBounce 			UInt8,\n"
			"FUniqID 				UInt64,\n"
			"OriginalURL 			String,\n"
			"HID 					UInt32,\n"
			"IsOldCounter 			UInt8,\n"
			"IsEvent 				UInt8,\n"
			"IsParameter 			UInt8,\n"
			"DontCountHits 			UInt8,\n"
			"WithHash 				UInt8\n"
			") ENGINE = Log";

		ParserCreateQuery parser;
		ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

		Context context;

		context.setPath("./");
		auto database = std::make_shared<DatabaseOrdinary>("test", "./metadata/test/");
		context.addDatabase("test", database);
		database->loadTables(context, nullptr);
		context.setCurrentDatabase("test");

		InterpreterCreateQuery interpreter(ast, context);
		interpreter.execute();
	}
	catch (const Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
	}

	return 0;
}
