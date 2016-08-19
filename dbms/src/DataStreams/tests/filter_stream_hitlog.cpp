#include <iostream>
#include <iomanip>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageLog.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>

#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ExpressionActions.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		NamesAndTypesList names_and_types_list
		{
			{"WatchID",				std::make_shared<DataTypeUInt64>()},
			{"JavaEnable",			std::make_shared<DataTypeUInt8>()},
			{"Title",				std::make_shared<DataTypeString>()},
			{"EventTime",			std::make_shared<DataTypeDateTime>()},
			{"CounterID",			std::make_shared<DataTypeUInt32>()},
			{"ClientIP",			std::make_shared<DataTypeUInt32>()},
			{"RegionID",			std::make_shared<DataTypeUInt32>()},
			{"UniqID",				std::make_shared<DataTypeUInt64>()},
			{"CounterClass",		std::make_shared<DataTypeUInt8>()},
			{"OS",					std::make_shared<DataTypeUInt8>()},
			{"UserAgent",			std::make_shared<DataTypeUInt8>()},
			{"URL",					std::make_shared<DataTypeString>()},
			{"Referer",				std::make_shared<DataTypeString>()},
			{"ResolutionWidth",		std::make_shared<DataTypeUInt16>()},
			{"ResolutionHeight",	std::make_shared<DataTypeUInt16>()},
			{"ResolutionDepth",		std::make_shared<DataTypeUInt8>()},
			{"FlashMajor",			std::make_shared<DataTypeUInt8>()},
			{"FlashMinor",			std::make_shared<DataTypeUInt8>()},
			{"FlashMinor2",			std::make_shared<DataTypeString>()},
			{"NetMajor",			std::make_shared<DataTypeUInt8>()},
			{"NetMinor",			std::make_shared<DataTypeUInt8>()},
			{"UserAgentMajor",		std::make_shared<DataTypeUInt16>()},
			{"UserAgentMinor",		std::make_shared<DataTypeFixedString>(2)},
			{"CookieEnable",		std::make_shared<DataTypeUInt8>()},
			{"JavascriptEnable",	std::make_shared<DataTypeUInt8>()},
			{"IsMobile",			std::make_shared<DataTypeUInt8>()},
			{"MobilePhone",			std::make_shared<DataTypeUInt8>()},
			{"MobilePhoneModel",	std::make_shared<DataTypeString>()},
			{"Params",				std::make_shared<DataTypeString>()},
			{"IPNetworkID",			std::make_shared<DataTypeUInt32>()},
			{"TraficSourceID",		std::make_shared<DataTypeInt8>()},
			{"SearchEngineID",		std::make_shared<DataTypeUInt16>()},
			{"SearchPhrase",		std::make_shared<DataTypeString>()},
			{"AdvEngineID",			std::make_shared<DataTypeUInt8>()},
			{"IsArtifical",			std::make_shared<DataTypeUInt8>()},
			{"WindowClientWidth",	std::make_shared<DataTypeUInt16>()},
			{"WindowClientHeight",	std::make_shared<DataTypeUInt16>()},
			{"ClientTimeZone",		std::make_shared<DataTypeInt16>()},
			{"ClientEventTime",		std::make_shared<DataTypeDateTime>()},
			{"SilverlightVersion1",	std::make_shared<DataTypeUInt8>()},
			{"SilverlightVersion2",	std::make_shared<DataTypeUInt8>()},
			{"SilverlightVersion3",	std::make_shared<DataTypeUInt32>()},
			{"SilverlightVersion4",	std::make_shared<DataTypeUInt16>()},
			{"PageCharset",			std::make_shared<DataTypeString>()},
			{"CodeVersion",			std::make_shared<DataTypeUInt32>()},
			{"IsLink",				std::make_shared<DataTypeUInt8>()},
			{"IsDownload",			std::make_shared<DataTypeUInt8>()},
			{"IsNotBounce",			std::make_shared<DataTypeUInt8>()},
			{"FUniqID",				std::make_shared<DataTypeUInt64>()},
			{"OriginalURL",			std::make_shared<DataTypeString>()},
			{"HID",					std::make_shared<DataTypeUInt32>()},
			{"IsOldCounter",		std::make_shared<DataTypeUInt8>()},
			{"IsEvent",				std::make_shared<DataTypeUInt8>()},
			{"IsParameter",			std::make_shared<DataTypeUInt8>()},
			{"DontCountHits",		std::make_shared<DataTypeUInt8>()},
			{"WithHash",			std::make_shared<DataTypeUInt8>()},
		};

		Context context;

		std::string input = "SELECT UniqID, URL, CounterID, IsLink WHERE URL = 'http://mail.yandex.ru/neo2/#inbox'";
		ParserSelectQuery parser;
		ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

		formatAST(*ast, std::cerr);
		std::cerr << std::endl;
		std::cerr << ast->getTreeID() << std::endl;

		/// создаём объект существующей таблицы хит лога

		StoragePtr table = StorageLog::create("./", "HitLog", std::make_shared<NamesAndTypesList>(names_and_types_list));

		/// читаем из неё, применяем выражение, фильтруем, и пишем в tsv виде в консоль

		ExpressionAnalyzer analyzer(ast, context, nullptr, names_and_types_list);
		ExpressionActionsChain chain;
		analyzer.appendSelect(chain, false);
		analyzer.appendWhere(chain, false);
		chain.finalize();
		ExpressionActionsPtr expression = chain.getLastActions();

		Names column_names
		{
			"UniqID",
			"URL",
			"CounterID",
			"IsLink",
		};

		QueryProcessingStage::Enum stage;

		BlockInputStreamPtr in = table->read(column_names, 0, context, Settings(), stage)[0];
		in = std::make_shared<FilterBlockInputStream>(in, expression, 4);
		//in = std::make_shared<LimitBlockInputStream>(in, 10, 0);

		WriteBufferFromOStream ob(std::cout);
		RowOutputStreamPtr out_ = std::make_shared<TabSeparatedRowOutputStream>(ob, expression->getSampleBlock());
		BlockOutputStreamFromRowOutputStream out(out_);

		copyData(*in, out);
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
