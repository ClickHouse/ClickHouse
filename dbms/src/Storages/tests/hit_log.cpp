#include <map>
#include <list>
#include <iostream>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumber.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDateTime.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Storages/StorageLog.h>

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

	DataTypes data_types;
	Names column_names;

	for (const auto & name_type : names_and_types_list)
	{
		data_types.push_back(name_type.type);
		column_names.push_back(name_type.name);
	}

	/// create a hit log table

	StoragePtr table = StorageLog::create("./", "HitLog", std::make_shared<NamesAndTypesList>(names_and_types_list));

	/// create a description of how to read data from the tab separated dump

	Block sample;
	for (const auto & name_type : names_and_types_list)
	{
		ColumnWithTypeAndName elem;
		elem.name = name_type.name;
		elem.type = name_type.type;
		elem.column = elem.type->createColumn();
		sample.insert(std::move(elem));
	}

	/// read the data from tsv file and simultaneously write to table
	if (argc == 2 && 0 == strcmp(argv[1], "write"))
	{
		ReadBufferFromIStream in_buf(std::cin);

		RowInputStreamPtr in_ = std::make_shared<TabSeparatedRowInputStream>(in_buf, sample);
		BlockInputStreamFromRowInputStream in(in_, sample, DEFAULT_INSERT_BLOCK_SIZE, 0, 0);
		BlockOutputStreamPtr out = table->write({}, {});
		copyData(in, *out);
	}

	/// read from it
	if (argc == 2 && 0 == strcmp(argv[1], "read"))
	{
		WriteBufferFromOStream out_buf(std::cout);

		QueryProcessingStage::Enum stage;

		BlockInputStreamPtr in = table->read(column_names, 0, Context{}, Settings(), stage)[0];
		RowOutputStreamPtr out_ = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample);
		BlockOutputStreamFromRowOutputStream out(out_);
		copyData(*in, out);
	}

	return 0;
}
catch (const Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl;
	throw;
}
