#include <DB/Common/Stopwatch.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTRenameQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterRenameQuery.h>
#include <DB/Interpreters/QueryLog.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>
#include <DB/Common/setThreadName.h>
#include <common/ClickHouseRevision.h>


namespace DB
{


QueryLog::QueryLog(Context & context_, const String & database_name_, const String & table_name_, size_t flush_interval_milliseconds_)
	: context(context_), database_name(database_name_), table_name(table_name_), flush_interval_milliseconds(flush_interval_milliseconds_)
{
	data.reserve(DBMS_QUERY_LOG_QUEUE_SIZE);

	{
		String description = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name);

		auto lock = context.getLock();

		table = context.tryGetTable(database_name, table_name);

		if (table)
		{
			const Block expected = createBlock();
			const Block actual = table->getSampleBlockNonMaterialized();

			if (!blocksHaveEqualStructure(actual, expected))
			{
				/// Переименовываем существующую таблицу.
				int suffix = 0;
				while (context.isTableExist(database_name, table_name + "_" + toString(suffix)))
					++suffix;

				auto rename = std::make_shared<ASTRenameQuery>();

				ASTRenameQuery::Table from;
				from.database = database_name;
				from.table = table_name;

				ASTRenameQuery::Table to;
				to.database = database_name;
				to.table = table_name + "_" + toString(suffix);

				ASTRenameQuery::Element elem;
				elem.from = from;
				elem.to = to;

				rename->elements.emplace_back(elem);

				LOG_DEBUG(log, "Existing table " << description << " for query log has obsolete or different structure."
					" Renaming it to " << backQuoteIfNeed(to.table));

				InterpreterRenameQuery(rename, context).execute();

				/// Нужная таблица будет создана.
				table = nullptr;
			}
			else
				LOG_DEBUG(log, "Will use existing table " << description << " for query log.");
		}

		if (!table)
		{
			/// Создаём таблицу.
			LOG_DEBUG(log, "Creating new table " << description << " for query log.");

			auto create = std::make_shared<ASTCreateQuery>();

			create->database = database_name;
			create->table = table_name;

			Block sample = createBlock();
			create->columns = InterpreterCreateQuery::formatColumns(sample.getColumnsList());

			String engine = "MergeTree(event_date, event_time, 8192)";
			ParserFunction engine_parser;

			create->storage = parseQuery(engine_parser, engine.data(), engine.data() + engine.size(), "ENGINE to create table for query log");

			InterpreterCreateQuery(create, context).execute();

			table = context.getTable(database_name, table_name);
		}
	}

	saving_thread = std::thread([this] { threadFunction(); });
}


QueryLog::~QueryLog()
{
	/// Говорим потоку, что надо завершиться.
	QueryLogElement elem;
	elem.type = QueryLogElement::SHUTDOWN;
	queue.push(elem);

	saving_thread.join();
}


void QueryLog::threadFunction()
{
	setThreadName("QueryLogFlush");

	Stopwatch time_after_last_write;
	bool first = true;

	while (true)
	{
		try
		{
			if (first)
			{
				time_after_last_write.restart();
				first = false;
			}

			QueryLogElement element;
			bool has_element = false;

			if (data.empty())
			{
				queue.pop(element);
				has_element = true;
			}
			else
			{
				size_t milliseconds_elapsed = time_after_last_write.elapsed() / 1000000;
				if (milliseconds_elapsed < flush_interval_milliseconds)
					has_element = queue.tryPop(element, flush_interval_milliseconds - milliseconds_elapsed);
			}

			if (has_element)
			{
				if (element.type == QueryLogElement::SHUTDOWN)
				{
					flush();
					break;
				}
				else
					data.push_back(element);
			}

			size_t milliseconds_elapsed = time_after_last_write.elapsed() / 1000000;
			if (milliseconds_elapsed >= flush_interval_milliseconds)
			{
				/// Записываем данные в таблицу.
				flush();
				time_after_last_write.restart();
			}
		}
		catch (...)
		{
			/// В случае ошибки теряем накопленные записи, чтобы не блокироваться.
			data.clear();
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
}


Block QueryLog::createBlock()
{
	return
	{
		{std::make_shared<ColumnUInt8>(), 	std::make_shared<DataTypeUInt8>(), 		"type"},
		{std::make_shared<ColumnUInt16>(), 	std::make_shared<DataTypeDate>(), 		"event_date"},
		{std::make_shared<ColumnUInt32>(), 	std::make_shared<DataTypeDateTime>(), 	"event_time"},
		{std::make_shared<ColumnUInt32>(), 	std::make_shared<DataTypeDateTime>(), 	"query_start_time"},
		{std::make_shared<ColumnUInt64>(), 	std::make_shared<DataTypeUInt64>(), 	"query_duration_ms"},

		{std::make_shared<ColumnUInt64>(), 	std::make_shared<DataTypeUInt64>(), 	"read_rows"},
		{std::make_shared<ColumnUInt64>(), 	std::make_shared<DataTypeUInt64>(), 	"read_bytes"},

		{std::make_shared<ColumnUInt64>(), 	std::make_shared<DataTypeUInt64>(), 	"result_rows"},
		{std::make_shared<ColumnUInt64>(), 	std::make_shared<DataTypeUInt64>(), 	"result_bytes"},

		{std::make_shared<ColumnUInt64>(), 	std::make_shared<DataTypeUInt64>(), 	"memory_usage"},

		{std::make_shared<ColumnString>(), 	std::make_shared<DataTypeString>(), 	"query"},
		{std::make_shared<ColumnString>(), 	std::make_shared<DataTypeString>(), 	"exception"},
		{std::make_shared<ColumnString>(), 	std::make_shared<DataTypeString>(), 	"stack_trace"},

		{std::make_shared<ColumnUInt8>(), 	std::make_shared<DataTypeUInt8>(), 		"interface"},
		{std::make_shared<ColumnUInt8>(), 	std::make_shared<DataTypeUInt8>(), 		"http_method"},
		{std::make_shared<ColumnFixedString>(16), std::make_shared<DataTypeFixedString>(16), "ip_address"},
		{std::make_shared<ColumnString>(), 	std::make_shared<DataTypeString>(), 	"user"},
		{std::make_shared<ColumnString>(), 	std::make_shared<DataTypeString>(), 	"query_id"},
		{std::make_shared<ColumnUInt32>(), 	std::make_shared<DataTypeUInt32>(), 	"revision"},
	};
}


void QueryLog::flush()
{
	try
	{
		LOG_TRACE(log, "Flushing query log");

		const auto & date_lut = DateLUT::instance();

		Block block = createBlock();

		for (const QueryLogElement & elem : data)
		{
			char ipv6_binary[16];
			if (Poco::Net::IPAddress::IPv6 == elem.ip_address.family())
			{
				memcpy(ipv6_binary, elem.ip_address.addr(), 16);
			}
			else if (Poco::Net::IPAddress::IPv4 == elem.ip_address.family())
			{
				/// Преобразуем в IPv6-mapped адрес.
				memset(ipv6_binary, 0, 10);
				ipv6_binary[10] = '\xFF';
				ipv6_binary[11] = '\xFF';
				memcpy(&ipv6_binary[12], elem.ip_address.addr(), 4);
			}
			else
				memset(ipv6_binary, 0, 16);

			size_t i = 0;

			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.type));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(date_lut.toDayNum(elem.event_time)));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.event_time));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.query_start_time));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.query_duration_ms));

			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.read_rows));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.read_bytes));

			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.result_rows));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.result_bytes));

			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.memory_usage));

			block.unsafeGetByPosition(i++).column.get()->insertData(elem.query.data(), elem.query.size());
			block.unsafeGetByPosition(i++).column.get()->insertData(elem.exception.data(), elem.exception.size());
			block.unsafeGetByPosition(i++).column.get()->insertData(elem.stack_trace.data(), elem.stack_trace.size());

			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.interface));
			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(elem.http_method));

			block.unsafeGetByPosition(i++).column.get()->insertData(ipv6_binary, 16);

			block.unsafeGetByPosition(i++).column.get()->insertData(elem.user.data(), elem.user.size());
			block.unsafeGetByPosition(i++).column.get()->insertData(elem.query_id.data(), elem.query_id.size());

			block.unsafeGetByPosition(i++).column.get()->insert(static_cast<UInt64>(ClickHouseRevision::get()));
		}

		/// Мы пишем не напрямую в таблицу, а используем InterpreterInsertQuery.
		/// Это нужно, чтобы поддержать наличие DEFAULT-столбцов в таблице.

		std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
		insert->database = database_name;
		insert->table = table_name;
		ASTPtr query_ptr(insert.release());

		InterpreterInsertQuery interpreter(query_ptr, context);
		BlockIO io = interpreter.execute();

		io.out->writePrefix();
		io.out->write(block);
		io.out->writeSuffix();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	/// В случае ошибки тоже очищаем накопленные записи, чтобы не блокироваться.
	data.clear();
}

}
