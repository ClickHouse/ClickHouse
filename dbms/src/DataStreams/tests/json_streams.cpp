#include <map>
#include <list>
#include <iostream>
#include <fstream>

#include <boost/assign/list_inserter.hpp>

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
#include <DB/DataStreams/copyData.h>

#include <DB/Storages/StorageLog.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		DB::NamesAndTypesListPtr names_and_types_list = new DB::NamesAndTypesList;

		boost::assign::push_back(*names_and_types_list)
			("WatchID",				new DB::DataTypeUInt64)
			("ClientIP",			new DB::DataTypeUInt32)
			("Referer",				new DB::DataTypeString)
			("URL",					new DB::DataTypeString)
			("IsLink",				new DB::DataTypeUInt8)
			("OriginalUserAgent",	new DB::DataTypeString)
			("EventTime",			new DB::DataTypeDateTime)
		;

		SharedPtr<DB::DataTypes> data_types = new DB::DataTypes;

		for (DB::NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
			data_types->push_back(it->second);

		DB::Block sample;
		for (DB::NamesAndTypesList::const_iterator it = names_and_types_list->begin(); it != names_and_types_list->end(); ++it)
		{
			DB::ColumnWithNameAndType elem;
			elem.name = it->first;
			elem.type = it->second;
			elem.column = elem.type->createColumn();
			sample.insert(elem);
		}
		
		{
			std::ifstream istr("json_test.in");
			std::ofstream ostr("json_test.out");

			DB::ReadBufferFromIStream in_buf(istr);
			DB::WriteBufferFromOStream out_buf(ostr);

			DB::TabSeparatedRowInputStream row_input(in_buf, sample, true, true);
			DB::JSONRowOutputStream row_output(out_buf, sample);

			DB::copyData(row_input, row_output);
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
