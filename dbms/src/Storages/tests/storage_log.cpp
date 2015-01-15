#include <iostream>

#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Storages/StorageLog.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Interpreters/Context.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		const size_t rows = 10000000;

		/// создаём таблицу с парой столбцов

		DB::NamesAndTypesListPtr names_and_types = new DB::NamesAndTypesList;
		names_and_types->push_back(DB::NameAndTypePair("a", new DB::DataTypeUInt64));
		names_and_types->push_back(DB::NameAndTypePair("b", new DB::DataTypeUInt8));

		DB::StoragePtr table = DB::StorageLog::create("./", "test", names_and_types);

		/// пишем в неё
		{
			DB::Block block;

			DB::ColumnWithNameAndType column1;
			column1.name = "a";
			column1.type = table->getDataTypeByName("a");
			column1.column = column1.type->createColumn();
			DB::ColumnUInt64::Container_t & vec1 = typeid_cast<DB::ColumnUInt64&>(*column1.column).getData();

			vec1.resize(rows);
			for (size_t i = 0; i < rows; ++i)
				vec1[i] = i;

			block.insert(column1);

			DB::ColumnWithNameAndType column2;
			column2.name = "b";
			column2.type = table->getDataTypeByName("b");
			column2.column = column2.type->createColumn();
			DB::ColumnUInt8::Container_t & vec2 = typeid_cast<DB::ColumnUInt8&>(*column2.column).getData();

			vec2.resize(rows);
			for (size_t i = 0; i < rows; ++i)
				vec2[i] = i * 2;

			block.insert(column2);

			SharedPtr<DB::IBlockOutputStream> out = table->write(0);
			out->write(block);
		}

		/// читаем из неё
		{
			DB::Names column_names;
			column_names.push_back("a");
			column_names.push_back("b");

			DB::QueryProcessingStage::Enum stage;

			SharedPtr<DB::IBlockInputStream> in = table->read(column_names, 0, DB::Context{}, DB::Settings(), stage)[0];

			DB::Block sample;
			{
				DB::ColumnWithNameAndType col;
				col.type = new DB::DataTypeUInt64;
				sample.insert(col);
			}
			{
				DB::ColumnWithNameAndType col;
				col.type = new DB::DataTypeUInt8;
				sample.insert(col);
			}

			DB::WriteBufferFromOStream out_buf(std::cout);

			DB::LimitBlockInputStream in_limit(in, 10);
			DB::RowOutputStreamPtr output_ = new DB::TabSeparatedRowOutputStream(out_buf, sample);
			DB::BlockOutputStreamFromRowOutputStream output(output_);

			DB::copyData(in_limit, output);
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
