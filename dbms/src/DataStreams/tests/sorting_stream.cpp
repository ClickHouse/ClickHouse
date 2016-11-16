#include <iostream>
#include <iomanip>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageLog.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDateTime.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/PartialSortingBlockInputStream.h>
#include <DB/DataStreams/MergeSortingBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/Interpreters/Context.h>


using namespace DB;


int main(int argc, char ** argv)
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

	using NamesAndTypesMap = std::map<String, DataTypePtr>;
	NamesAndTypesMap names_and_types_map;

	for (const auto & name_type : names_and_types_list)
		names_and_types_map.emplace(name_type.name, name_type.type);

	std::string input = "SELECT UniqID, URL, CounterID, IsLink";
	ParserSelectQuery parser;
	ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

	formatAST(*ast, std::cerr);
	std::cerr << std::endl;

	/// создаём объект существующей таблицы хит лога

	StoragePtr table = StorageLog::create("./", "HitLog", std::make_shared<NamesAndTypesList>(names_and_types_list));

	/// читаем из неё, сортируем, и пишем в tsv виде в консоль

	Names column_names
	{
		"UniqID",
		"URL",
		"CounterID",
		"IsLink",
	};

	DataTypes result_types = DataTypes
	{
		names_and_types_map["UniqID"],
		names_and_types_map["URL"],
		names_and_types_map["CounterID"],
		names_and_types_map["IsLink"],
	};

	Block sample;
	for (const auto & type : result_types)
	{
		ColumnWithTypeAndName col;
		col.type = type;
		sample.insert(std::move(col));
	}

	SortDescription sort_columns;
	sort_columns.push_back(SortColumnDescription(1, -1));
	sort_columns.push_back(SortColumnDescription(2, 1));
	sort_columns.push_back(SortColumnDescription(0, 1));
	sort_columns.push_back(SortColumnDescription(3, 1));

	QueryProcessingStage::Enum stage;

	BlockInputStreamPtr in = table->read(column_names, 0, Context{}, Settings(), stage, argc == 2 ? atoi(argv[1]) : 1048576)[0];
	in = std::make_shared<PartialSortingBlockInputStream>(in, sort_columns);
	in = std::make_shared<MergeSortingBlockInputStream>(in, sort_columns, DEFAULT_BLOCK_SIZE, 0, 0, "");
	//in = std::make_shared<LimitBlockInputStream>(in, 10, 0);

	WriteBufferFromOStream ob(std::cout);
	RowOutputStreamPtr out_ = std::make_shared<TabSeparatedRowOutputStream>(ob, sample);
	BlockOutputStreamFromRowOutputStream out(out_);

	copyData(*in, out);

	return 0;
}
catch (const Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl;
	throw;
}
