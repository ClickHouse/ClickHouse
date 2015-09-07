#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

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


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		NamesAndTypesListPtr names_and_types_list = new NamesAndTypesList
		{
			{"WatchID",				new DataTypeUInt64},
			{"JavaEnable",			new DataTypeUInt8},
			{"Title",				new DataTypeString},
			{"EventTime",			new DataTypeDateTime},
			{"CounterID",			new DataTypeUInt32},
			{"ClientIP",			new DataTypeUInt32},
			{"RegionID",			new DataTypeUInt32},
			{"UniqID",				new DataTypeUInt64},
			{"CounterClass",		new DataTypeUInt8},
			{"OS",					new DataTypeUInt8},
			{"UserAgent",			new DataTypeUInt8},
			{"URL",					new DataTypeString},
			{"Referer",				new DataTypeString},
			{"ResolutionWidth",		new DataTypeUInt16},
			{"ResolutionHeight",	new DataTypeUInt16},
			{"ResolutionDepth",		new DataTypeUInt8},
			{"FlashMajor",			new DataTypeUInt8},
			{"FlashMinor",			new DataTypeUInt8},
			{"FlashMinor2",			new DataTypeString},
			{"NetMajor",			new DataTypeUInt8},
			{"NetMinor",			new DataTypeUInt8},
			{"UserAgentMajor",		new DataTypeUInt16},
			{"UserAgentMinor",		new DataTypeFixedString(2)},
			{"CookieEnable",		new DataTypeUInt8},
			{"JavascriptEnable",	new DataTypeUInt8},
			{"IsMobile",			new DataTypeUInt8},
			{"MobilePhone",			new DataTypeUInt8},
			{"MobilePhoneModel",	new DataTypeString},
			{"Params",				new DataTypeString},
			{"IPNetworkID",			new DataTypeUInt32},
			{"TraficSourceID",		new DataTypeInt8},
			{"SearchEngineID",		new DataTypeUInt16},
			{"SearchPhrase",		new DataTypeString},
			{"AdvEngineID",			new DataTypeUInt8},
			{"IsArtifical",			new DataTypeUInt8},
			{"WindowClientWidth",	new DataTypeUInt16},
			{"WindowClientHeight",	new DataTypeUInt16},
			{"ClientTimeZone",		new DataTypeInt16},
			{"ClientEventTime",		new DataTypeDateTime},
			{"SilverlightVersion1",	new DataTypeUInt8},
			{"SilverlightVersion2",	new DataTypeUInt8},
			{"SilverlightVersion3",	new DataTypeUInt32},
			{"SilverlightVersion4",	new DataTypeUInt16},
			{"PageCharset",			new DataTypeString},
			{"CodeVersion",			new DataTypeUInt32},
			{"IsLink",				new DataTypeUInt8},
			{"IsDownload",			new DataTypeUInt8},
			{"IsNotBounce",			new DataTypeUInt8},
			{"FUniqID",				new DataTypeUInt64},
			{"OriginalURL",			new DataTypeString},
			{"HID",					new DataTypeUInt32},
			{"IsOldCounter",		new DataTypeUInt8},
			{"IsEvent",				new DataTypeUInt8},
			{"IsParameter",			new DataTypeUInt8},
			{"DontCountHits",		new DataTypeUInt8},
			{"WithHash",			new DataTypeUInt8},
		};

		Context context;

		std::string input = "SELECT UniqID, URL, CounterID, IsLink WHERE URL = 'http://mail.yandex.ru/neo2/#inbox'";
		ParserSelectQuery parser;
		ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

		formatAST(*ast, std::cerr);
		std::cerr << std::endl;
		std::cerr << ast->getTreeID() << std::endl;

		/// создаём объект существующей таблицы хит лога

		StoragePtr table = StorageLog::create("./", "HitLog", names_and_types_list);

		/// читаем из неё, применяем выражение, фильтруем, и пишем в tsv виде в консоль

		ExpressionAnalyzer analyzer(ast, context, nullptr, *names_and_types_list);
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

		Poco::SharedPtr<IBlockInputStream> in = table->read(column_names, 0, context, Settings(), stage)[0];
		in = new ExpressionBlockInputStream(in, expression);
		in = new FilterBlockInputStream(in, 4);
		//in = new LimitBlockInputStream(in, 10, 0);

		WriteBufferFromOStream ob(std::cout);
		RowOutputStreamPtr out_ = new TabSeparatedRowOutputStream(ob, expression->getSampleBlock());
		BlockOutputStreamFromRowOutputStream out(out_);

		copyData(*in, out);

		//profiling->getInfo().print(std::cerr);
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
