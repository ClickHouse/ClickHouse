#include <iostream>

#include <DB/Storages/StorageMergeTree.h>

#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/parseQuery.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		const size_t rows = 12345;

		Context context;

		/// создаём таблицу с парой столбцов

		NamesAndTypesListPtr names_and_types = new NamesAndTypesList;
		names_and_types->push_back(NameAndTypePair("d", new DataTypeDate));
		names_and_types->push_back(NameAndTypePair("a", new DataTypeArray(new DataTypeUInt32)));

		ASTPtr primary_expr;
		Expected expected = "";
		String primary_expr_str = "d";
		const char * begin = primary_expr_str.data();
		const char * end = begin + primary_expr_str.size();
		const char * max_parsed_pos = begin;
		ParserExpressionList parser(false);
		if (!parser.parse(begin, end, primary_expr, max_parsed_pos, expected))
			throw Poco::Exception("Cannot parse " + primary_expr_str);

		StoragePtr table = StorageMergeTree::create(
			"./", "default", "test",
			names_and_types, {}, {}, ColumnDefaults{},
			context, primary_expr, "d",
			nullptr, 101, MergeTreeData::Ordinary, {}, {}, {});

		/// пишем в неё
		{
			Block block;

			ColumnWithTypeAndName column1;
			column1.name = "d";
			column1.type = table->getDataTypeByName("d");
			column1.column = column1.type->createColumn();
			ColumnUInt16::Container_t & vec1 = typeid_cast<ColumnUInt16 &>(*column1.column).getData();

			vec1.resize(rows);
			for (size_t i = 0; i < rows; ++i)
				vec1[i] = 10000;

			block.insert(column1);

			ColumnWithTypeAndName column2;
			column2.name = "a";
			column2.type = table->getDataTypeByName("a");
			column2.column = column2.type->createColumn();

			for (size_t i = 0; i < rows; ++i)
				column2.column->insert(Array((rand() % 10) == 0 ? (rand() % 10) : 0, i));

			block.insert(column2);

			SharedPtr<IBlockOutputStream> out = table->write({}, {});
			out->write(block);
		}

		/// читаем из неё
		{
			Names column_names;
			column_names.push_back("d");
			column_names.push_back("a");

			QueryProcessingStage::Enum stage;

			ASTPtr select;
			Expected expected = "";
			String select_str = "SELECT * FROM test";
			const char * begin = select_str.data();
			const char * end = begin + select_str.size();
			const char * max_parsed_pos = begin;
			ParserSelectQuery parser;
			if (!parser.parse(begin, end, select, max_parsed_pos, expected))
				throw Poco::Exception("Cannot parse " + primary_expr_str);

			SharedPtr<IBlockInputStream> in = table->read(column_names, select, context, Settings(), stage)[0];

			Block sample;
			{
				ColumnWithTypeAndName col;
				col.type = names_and_types->front().type;
				sample.insert(col);
			}
			{
				ColumnWithTypeAndName col;
				col.type = names_and_types->back().type;
				sample.insert(col);
			}

			WriteBufferFromFileDescriptor out_buf(STDOUT_FILENO);

			RowOutputStreamPtr output_ = new TabSeparatedRowOutputStream(out_buf, sample);
			BlockOutputStreamFromRowOutputStream output(output_);

			copyData(*in, output);
		}
	}
	catch (const Exception & e)
	{
		std::cerr << e.displayText() << ", stack trace: \n\n" << e.getStackTrace().toString() << std::endl;
		return 1;
	}

	return 0;
}
