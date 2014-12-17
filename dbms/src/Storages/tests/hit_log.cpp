#include <map>
#include <list>
#include <iostream>

#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
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


using Poco::SharedPtr;
using namespace DB;


int main(int argc, char ** argv)
{
	try
	{
		DB::NamesAndTypesListPtr names_and_types_list = new DB::NamesAndTypesList
		{
			{"WatchID",				new DB::DataTypeUInt64},
			{"JavaEnable",			new DB::DataTypeUInt8},
			{"Title",				new DB::DataTypeString},
			{"EventTime",			new DB::DataTypeDateTime},
			{"CounterID",			new DB::DataTypeUInt32},
			{"ClientIP",			new DB::DataTypeUInt32},
			{"RegionID",			new DB::DataTypeUInt32},
			{"UniqID",				new DB::DataTypeUInt64},
			{"CounterClass",		new DB::DataTypeUInt8},
			{"OS",					new DB::DataTypeUInt8},
			{"UserAgent",			new DB::DataTypeUInt8},
			{"URL",					new DB::DataTypeString},
			{"Referer",				new DB::DataTypeString},
			{"ResolutionWidth",		new DB::DataTypeUInt16},
			{"ResolutionHeight",	new DB::DataTypeUInt16},
			{"ResolutionDepth",		new DB::DataTypeUInt8},
			{"FlashMajor",			new DB::DataTypeUInt8},
			{"FlashMinor",			new DB::DataTypeUInt8},
			{"FlashMinor2",			new DB::DataTypeString},
			{"NetMajor",			new DB::DataTypeUInt8},
			{"NetMinor",			new DB::DataTypeUInt8},
			{"UserAgentMajor",		new DB::DataTypeUInt16},
			{"UserAgentMinor",		new DB::DataTypeFixedString(2)},
			{"CookieEnable",		new DB::DataTypeUInt8},
			{"JavascriptEnable",	new DB::DataTypeUInt8},
			{"IsMobile",			new DB::DataTypeUInt8},
			{"MobilePhone",			new DB::DataTypeUInt8},
			{"MobilePhoneModel",	new DB::DataTypeString},
			{"Params",				new DB::DataTypeString},
			{"IPNetworkID",			new DB::DataTypeUInt32},
			{"TraficSourceID",		new DB::DataTypeInt8},
			{"SearchEngineID",		new DB::DataTypeUInt16},
			{"SearchPhrase",		new DB::DataTypeString},
			{"AdvEngineID",			new DB::DataTypeUInt8},
			{"IsArtifical",			new DB::DataTypeUInt8},
			{"WindowClientWidth",	new DB::DataTypeUInt16},
			{"WindowClientHeight",	new DB::DataTypeUInt16},
			{"ClientTimeZone",		new DB::DataTypeInt16},
			{"ClientEventTime",		new DB::DataTypeDateTime},
			{"SilverlightVersion1",	new DB::DataTypeUInt8},
			{"SilverlightVersion2",	new DB::DataTypeUInt8},
			{"SilverlightVersion3",	new DB::DataTypeUInt32},
			{"SilverlightVersion4",	new DB::DataTypeUInt16},
			{"PageCharset",			new DB::DataTypeString},
			{"CodeVersion",			new DB::DataTypeUInt32},
			{"IsLink",				new DB::DataTypeUInt8},
			{"IsDownload",			new DB::DataTypeUInt8},
			{"IsNotBounce",			new DB::DataTypeUInt8},
			{"FUniqID",				new DB::DataTypeUInt64},
			{"OriginalURL",			new DB::DataTypeString},
			{"HID",					new DB::DataTypeUInt32},
			{"IsOldCounter",		new DB::DataTypeUInt8},
			{"IsEvent",				new DB::DataTypeUInt8},
			{"IsParameter",			new DB::DataTypeUInt8},
			{"DontCountHits",		new DB::DataTypeUInt8},
			{"WithHash",			new DB::DataTypeUInt8},
		};

		SharedPtr<DataTypes> data_types = new DataTypes;
		Names column_names;

		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
		{
			data_types->push_back(it->type);
			column_names.push_back(it->name);
		}

		/// создаём таблицу хит лога

		StoragePtr table = StorageLog::create("./", "HitLog", names_and_types_list);

		/// создаём описание, как читать данные из tab separated дампа

		Block sample;
		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
		{
			ColumnWithNameAndType elem;
			elem.name = it->name;
			elem.type = it->type;
			elem.column = elem.type->createColumn();
			sample.insert(elem);
		}

		/// читаем данные из tsv файла и одновременно пишем в таблицу
		if (argc == 2 && 0 == strcmp(argv[1], "write"))
		{
			ReadBufferFromIStream in_buf(std::cin);

			RowInputStreamPtr in_ = new TabSeparatedRowInputStream(in_buf, sample);
			BlockInputStreamFromRowInputStream in(in_, sample);
			BlockOutputStreamPtr out = table->write(0);
			copyData(in, *out);
		}

		/// читаем из неё
		if (argc == 2 && 0 == strcmp(argv[1], "read"))
		{
			WriteBufferFromOStream out_buf(std::cout);

			QueryProcessingStage::Enum stage;

			BlockInputStreamPtr in = table->read(column_names, 0, Context{}, Settings(), stage)[0];
			RowOutputStreamPtr out_ = new TabSeparatedRowOutputStream(out_buf, sample);
			BlockOutputStreamFromRowOutputStream out(out_);
			copyData(*in, out);
		}
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
