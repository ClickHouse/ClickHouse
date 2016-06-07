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
#include <DB/DataStreams/TabSeparatedBlockOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Storages/StorageLog.h>


using Poco::SharedPtr;
using namespace DB;


int main(int argc, char ** argv)
{
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

		SharedPtr<DataTypes> data_types = new DataTypes;

		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
			data_types->push_back(it->type);

		/// создаём описание, как читать данные из tab separated дампа

		Block sample;
		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
		{
			ColumnWithTypeAndName elem;
			elem.name = it->name;
			elem.type = it->type;
			elem.column = elem.type->createColumn();
			sample.insert(elem);
		}

		/// читаем данные из строчного tsv файла и одновременно пишем в блочный tsv файл
		{
			ReadBufferFromIStream in_buf(std::cin);
			WriteBufferFromOStream out_buf(std::cout);

			RowInputStreamPtr row_in = new TabSeparatedRowInputStream(in_buf, sample);
			BlockInputStreamFromRowInputStream in(row_in, sample);
			TabSeparatedBlockOutputStream out(out_buf);
			copyData(in, out);
		}
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
