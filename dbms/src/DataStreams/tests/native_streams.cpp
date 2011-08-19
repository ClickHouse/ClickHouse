#include <string>

#include <iostream>
#include <fstream>

#include <boost/assign/list_inserter.hpp>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeFactory.h>

#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Storages/StorageLog.h>


int main(int argc, char ** argv)
{
	using Poco::SharedPtr;
	
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
		DB::Names column_names;

		for (NamesAndTypesList::const_iterator it = names_and_types_list.begin(); it != names_and_types_list.end(); ++it)
		{
			names_and_types_map->insert(*it);
			column_names.push_back(it->first);
		}

		/// создаём объект существующей таблицы хит лога

		DB::StorageLog table("./", "HitLog", names_and_types_map, ".bin");

		/// читаем из неё
		if (argc == 2 && 0 == strcmp(argv[1], "read"))
		{
			SharedPtr<DB::IBlockInputStream> in = table.read(column_names, 0);
			DB::WriteBufferFromOStream out1(std::cout);
			DB::CompressedWriteBuffer out2(out1);
			DB::NativeBlockOutputStream out3(out2);
			DB::copyData(*in, out3);
		}

		/// читаем данные из native файла и одновременно пишем в таблицу
		if (argc == 2 && 0 == strcmp(argv[1], "write"))
		{
			DB::DataTypeFactory factory;
			
			DB::ReadBufferFromIStream in1(std::cin);
			DB::CompressedReadBuffer in2(in1);
			DB::NativeBlockInputStream in3(in2, factory);
			SharedPtr<DB::IBlockOutputStream> out = table.write(0);
			DB::copyData(in3, *out);
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
