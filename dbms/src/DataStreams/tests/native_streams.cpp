#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeFactory.h>

#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Storages/StorageLog.h>

#include <DB/Interpreters/Context.h>
#include <Yandex/Revision.h>


int main(int argc, char ** argv)
{
	using Poco::SharedPtr;
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

		Names column_names;

		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
			column_names.push_back(it->name);

		/// создаём объект существующей таблицы хит лога

		StoragePtr table = StorageLog::create("./", "HitLog", names_and_types_list);

		/// читаем из неё
		if (argc == 2 && 0 == strcmp(argv[1], "read"))
		{
			QueryProcessingStage::Enum stage;
			SharedPtr<IBlockInputStream> in = table->read(column_names, 0, Context{}, Settings(), stage)[0];
			WriteBufferFromFileDescriptor out1(STDOUT_FILENO);
			CompressedWriteBuffer out2(out1);
			NativeBlockOutputStream out3(out2, Revision::get());
			copyData(*in, out3);
		}

		/// читаем данные из native файла и одновременно пишем в таблицу
		if (argc == 2 && 0 == strcmp(argv[1], "write"))
		{
			DataTypeFactory factory;

			ReadBufferFromFileDescriptor in1(STDIN_FILENO);
			CompressedReadBuffer in2(in1);
			NativeBlockInputStream in3(in2, factory, Revision::get());
			SharedPtr<IBlockOutputStream> out = table->write(0);
			copyData(in3, *out);
		}
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
