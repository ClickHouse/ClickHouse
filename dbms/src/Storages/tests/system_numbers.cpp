#include <iostream>

#include <Poco/SharedPtr.h>

#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		DB::StorageSystemNumbers table;

		DB::ColumnNames column_names;
		column_names.push_back("number");

		Poco::SharedPtr<DB::DataTypes> column_types = new DB::DataTypes;
		column_types->push_back(new DB::DataTypeUInt64);
		
		DB::LimitBlockInputStream input(table.read(column_names, 0, 10), 10, 96);
		DB::TabSeparatedRowOutputStream output(std::cout, column_types);
		
		DB::copyData(input, output);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
