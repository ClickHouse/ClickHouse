#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Storages/System/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/BlockExtraInfoInputStream.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>

#include <DB/Interpreters/Context.h>

using Poco::SharedPtr;

void test1()
{
	DB::StoragePtr table = DB::StorageSystemNumbers::create("numbers");

	DB::Names column_names;
	column_names.push_back("number");

	DB::QueryProcessingStage::Enum stage1;
	DB::QueryProcessingStage::Enum stage2;
	DB::QueryProcessingStage::Enum stage3;

	DB::BlockInputStreams streams;
	streams.emplace_back(new DB::LimitBlockInputStream(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage1, 1)[0], 30, 30000));
	streams.emplace_back(new DB::LimitBlockInputStream(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage2, 1)[0], 30, 2000));
	streams.emplace_back(new DB::LimitBlockInputStream(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage3, 1)[0], 30, 100));

	DB::UnionBlockInputStream<> union_stream(streams, nullptr, 2);

	DB::FormatFactory format_factory;
	DB::WriteBufferFromFileDescriptor wb(STDERR_FILENO);
	DB::Block sample = table->getSampleBlock();
	DB::BlockOutputStreamPtr out = format_factory.getOutput("TabSeparated", wb, sample);

	while (DB::Block block = union_stream.read())
	{
		out->write(block);
		wb.next();
	}
	//DB::copyData(union_stream, *out);
}

void test2()
{
	DB::StoragePtr table = DB::StorageSystemNumbers::create("numbers");

	DB::Names column_names;
	column_names.push_back("number");

	DB::QueryProcessingStage::Enum stage1;
	DB::QueryProcessingStage::Enum stage2;
	DB::QueryProcessingStage::Enum stage3;

	DB::BlockExtraInfo extra_info1;
	extra_info1.host = "host1";
	extra_info1.resolved_address = "127.0.0.1";
	extra_info1.port = 9000;
	extra_info1.user = "user1";

	DB::BlockExtraInfo extra_info2;
	extra_info2.host = "host2";
	extra_info2.resolved_address = "127.0.0.2";
	extra_info2.port = 9001;
	extra_info2.user = "user2";

	DB::BlockExtraInfo extra_info3;
	extra_info3.host = "host3";
	extra_info3.resolved_address = "127.0.0.3";
	extra_info3.port = 9003;
	extra_info3.user = "user3";

	DB::BlockInputStreams streams;

	DB::BlockInputStreamPtr stream1 = new DB::LimitBlockInputStream(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage1, 1)[0], 30, 30000);
	stream1 = new DB::BlockExtraInfoInputStream(stream1, extra_info1);
	streams.emplace_back(stream1);

	DB::BlockInputStreamPtr stream2 = new DB::LimitBlockInputStream(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage2, 1)[0], 30, 2000);
	stream2 = new DB::BlockExtraInfoInputStream(stream2, extra_info2);
	streams.emplace_back(stream2);

	DB::BlockInputStreamPtr stream3 = new DB::LimitBlockInputStream(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage3, 1)[0], 30, 100);
	stream3 = new DB::BlockExtraInfoInputStream(stream3, extra_info3);
	streams.emplace_back(stream3);

	DB::UnionBlockInputStream<DB::StreamUnionMode::ExtraInfo> union_stream(streams, nullptr, 2);

	auto getSampleBlock = []()
	{
		DB::Block block;
		DB::ColumnWithTypeAndName col;

		col.name = "number";
		col.type = new DB::DataTypeUInt64;
		col.column = col.type->createColumn();
		block.insert(col);

		col.name = "host_name";
		col.type = new DB::DataTypeString;
		col.column = col.type->createColumn();
		block.insert(col);

		col.name = "host_address";
		col.type = new DB::DataTypeString;
		col.column = col.type->createColumn();
		block.insert(col);

		col.name = "port";
		col.type = new DB::DataTypeUInt16;
		col.column = col.type->createColumn();
		block.insert(col);

		col.name = "user";
		col.type = new DB::DataTypeString;
		col.column = col.type->createColumn();
		block.insert(col);

		return block;
	};

	DB::FormatFactory format_factory;
	DB::WriteBufferFromFileDescriptor wb(STDERR_FILENO);
	DB::Block sample = getSampleBlock();
	DB::BlockOutputStreamPtr out = format_factory.getOutput("TabSeparated", wb, sample);

	while (DB::Block block = union_stream.read())
	{
		const auto & col = block.getByPosition(0);
		auto extra_info = union_stream.getBlockExtraInfo();

		DB::ColumnPtr host_name_column = new DB::ColumnString;
		DB::ColumnPtr host_address_column = new DB::ColumnString;
		DB::ColumnPtr port_column = new DB::ColumnUInt16;
		DB::ColumnPtr user_column = new DB::ColumnString;

		size_t row_count = block.rows();
		for (size_t i = 0; i < row_count; ++i)
		{
			host_name_column->insert(extra_info.resolved_address);
			host_address_column->insert(extra_info.host);
			port_column->insert(static_cast<UInt64>(extra_info.port));
			user_column->insert(extra_info.user);
		}

		DB::Block out_block;
		out_block.insert(DB::ColumnWithTypeAndName(col.column->clone(), col.type, col.name));
		out_block.insert(DB::ColumnWithTypeAndName(host_name_column, new DB::DataTypeString, "host_name"));
		out_block.insert(DB::ColumnWithTypeAndName(host_address_column, new DB::DataTypeString, "host_address"));
		out_block.insert(DB::ColumnWithTypeAndName(port_column, new DB::DataTypeUInt16, "port"));
		out_block.insert(DB::ColumnWithTypeAndName(user_column, new DB::DataTypeString, "user"));

		out->write(out_block);
		wb.next();
	}
	//DB::copyData(union_stream, *out);
}

int main(int argc, char ** argv)
{
	try
	{
		test1();
		test2();
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString();
		return 1;
	}

	return 0;
}
