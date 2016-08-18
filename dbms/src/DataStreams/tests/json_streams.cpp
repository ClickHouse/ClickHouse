#include <map>
#include <list>
#include <iostream>
#include <fstream>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDateTime.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedBlockOutputStream.h>
#include <DB/DataStreams/JSONRowOutputStream.h>
#include <DB/DataStreams/JSONCompactRowOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Storages/StorageLog.h>


using namespace DB;


int main(int argc, char ** argv)
try
{
	NamesAndTypesList names_and_types_list
	{
		{"WatchID",				std::make_shared<DataTypeUInt64>()},
		{"ClientIP",			std::make_shared<DataTypeUInt32>()},
		{"Referer",				std::make_shared<DataTypeString>()},
		{"URL",					std::make_shared<DataTypeString>()},
		{"IsLink",				std::make_shared<DataTypeUInt8>()},
		{"OriginalUserAgent",	std::make_shared<DataTypeString>()},
		{"EventTime",			std::make_shared<DataTypeDateTime>()},
	};

	Block sample;
	for (const auto & name_type : names_and_types_list)
	{
		ColumnWithTypeAndName elem;
		elem.name = name_type.name;
		elem.type = name_type.type;
		elem.column = elem.type->createColumn();
		sample.insert(std::move(elem));
	}

	{
		std::ifstream istr("json_test.in");
		std::ofstream ostr("json_test.out");

		ReadBufferFromIStream in_buf(istr);
		WriteBufferFromOStream out_buf(ostr);

		//TabSeparatedRowInputStream row_input(in_buf, sample, true, true);
		//JSONRowOutputStream row_output(out_buf, sample);
		//JSONCompactRowOutputStream row_output(out_buf, sample);
		//copyData(row_input, row_output);

		BlockInputStreamFromRowInputStream in(std::make_shared<TabSeparatedRowInputStream>(in_buf, sample, true, true), sample);
		BlockOutputStreamFromRowOutputStream out(std::make_shared<JSONRowOutputStream>(out_buf, sample, false));
		copyData(in, out);
	}

	return 0;
}
catch (const Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl;
	throw;
}
