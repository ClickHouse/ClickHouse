#include <map>
#include <list>
#include <iostream>
#include <fstream>

#include <Poco/SharedPtr.h>

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


using Poco::SharedPtr;
using namespace DB;


int main(int argc, char ** argv)
{
	try
	{
		NamesAndTypesListPtr names_and_types_list = new NamesAndTypesList
		{
			{"WatchID",				new DataTypeUInt64},
			{"ClientIP",			new DataTypeUInt32},
			{"Referer",				new DataTypeString},
			{"URL",					new DataTypeString},
			{"IsLink",				new DataTypeUInt8},
			{"OriginalUserAgent",	new DataTypeString},
			{"EventTime",			new DataTypeDateTime},
		};

		SharedPtr<DataTypes> data_types = new DataTypes;

		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
			data_types->push_back(it->type);

		Block sample;
		for (NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
		{
			ColumnWithNameAndType elem;
			elem.name = it->name;
			elem.type = it->type;
			elem.column = elem.type->createColumn();
			sample.insert(elem);
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

			BlockInputStreamFromRowInputStream in(new TabSeparatedRowInputStream (in_buf, sample, true, true), sample);
			BlockOutputStreamFromRowOutputStream out(new JSONRowOutputStream(out_buf, sample));
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
