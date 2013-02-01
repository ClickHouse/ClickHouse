#include <iostream>
#include <iomanip>

#include <boost/assign/list_inserter.hpp>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/NumberParser.h>

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
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		DB::NamesAndTypesListPtr names_and_types_list = new DB::NamesAndTypesList;

		boost::assign::push_back(*names_and_types_list)
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

		typedef std::map<DB::String, DB::DataTypePtr> NamesAndTypesMap;
		SharedPtr<NamesAndTypesMap> names_and_types_map = new NamesAndTypesMap;

		for (DB::NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
			names_and_types_map->insert(*it);

		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input = "SELECT UniqID, URL, CounterID, IsLink";
		std::string expected;

		const char * begin = input.data();
		const char * end = begin + input.size();
		const char * pos = begin;

		if (!parser.parse(pos, end, ast, expected))
		{
			std::cout << "Failed at position " << (pos - begin) << ": "
				<< mysqlxx::quote << input.substr(pos - begin, 10)
				<< ", expected " << expected << "." << std::endl;
		}

		DB::formatAST(*ast, std::cerr);
		std::cerr << std::endl;
		std::cerr << ast->getTreeID() << std::endl;

		/// создаём объект существующей таблицы хит лога

		DB::StorageLog table("./", "HitLog", names_and_types_list);

		/// читаем из неё, сортируем, и пишем в tsv виде в консоль

		DB::Names column_names;
		boost::assign::push_back(column_names)
			("UniqID")
			("URL")
			("CounterID")
			("IsLink")
		;

		Poco::SharedPtr<DB::DataTypes> result_types = new DB::DataTypes;
		boost::assign::push_back(*result_types)
			((*names_and_types_map)["UniqID"])
			((*names_and_types_map)["URL"])
			((*names_and_types_map)["CounterID"])
			((*names_and_types_map)["IsLink"])
		;

		DB::Block sample;
		for (DB::DataTypes::const_iterator it = result_types->begin(); it != result_types->end(); ++it)
		{
			DB::ColumnWithNameAndType col;
			col.type = *it;
			sample.insert(col);
		}

		DB::SortDescription sort_columns;
		sort_columns.push_back(DB::SortColumnDescription(1, -1));
		sort_columns.push_back(DB::SortColumnDescription(2, 1));
		sort_columns.push_back(DB::SortColumnDescription(0, 1));
		sort_columns.push_back(DB::SortColumnDescription(3, 1));

		DB::QueryProcessingStage::Enum stage;
		
		Poco::SharedPtr<DB::IBlockInputStream> in = table.read(column_names, 0, DB::Settings(), stage, argc == 2 ? atoi(argv[1]) : 1048576)[0];
		in = new DB::PartialSortingBlockInputStream(in, sort_columns);
		in = new DB::MergeSortingBlockInputStream(in, sort_columns);
		//in = new DB::LimitBlockInputStream(in, 10);

		DB::WriteBufferFromOStream ob(std::cout);
		DB::TabSeparatedRowOutputStream out(ob, sample);

		DB::copyData(*in, out);

/*		std::cerr << std::endl << "Reading: " << std::endl;
		profiling1->getInfo().print(std::cerr);
		std::cerr << std::endl << "Sorting: " << std::endl;
		profiling2->getInfo().print(std::cerr);
		std::cerr << std::endl << "Merging: " << std::endl;
		profiling3->getInfo().print(std::cerr);*/
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
