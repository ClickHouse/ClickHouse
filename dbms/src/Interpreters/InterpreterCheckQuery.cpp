#include <DB/Interpreters/InterpreterCheckQuery.h>
#include <DB/Parsers/ASTCheckQuery.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <openssl/sha.h>
#include <deque>
#include <array>

namespace DB
{

namespace ErrorCodes
{
	extern const int INVALID_BLOCK_EXTRA_INFO;
	extern const int RECEIVED_EMPTY_DATA;
}


namespace
{

/// Вспомогательная структура для оформления ответа на запрос DESCRIBE TABLE с Distributed-таблицей.
/// Содержит информацию про локальную таблицу, которая была получена с одной реплики.
struct TableDescription
{
	TableDescription(const Block & block, const BlockExtraInfo & extra_info_)
		: extra_info(extra_info_)
	{
		const auto & name_column = typeid_cast<const ColumnString &>(*block.getByName("name").column);
		const auto & type_column = typeid_cast<const ColumnString &>(*block.getByName("type").column);
		const auto & default_type_column = typeid_cast<const ColumnString &>(*block.getByName("default_type").column);
		const auto & default_expression_column = typeid_cast<const ColumnString &>(*block.getByName("default_expression").column);

		size_t row_count = block.rows();

		names_with_types.reserve(name_column.byteSize() + type_column.byteSize() + (3 * row_count));

		SHA512_CTX ctx;
		SHA512_Init(&ctx);

		bool is_first = true;
		for (size_t i = 0; i < row_count; ++i)
		{
			const auto & name = name_column.getDataAt(i).toString();
			const auto & type = type_column.getDataAt(i).toString();
			const auto & default_type = default_type_column.getDataAt(i).toString();
			const auto & default_expression = default_expression_column.getDataAt(i).toString();

			names_with_types.append(is_first ? "" : ", ");
			names_with_types.append(name);
			names_with_types.append(" ");
			names_with_types.append(type);

			SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(name.data()), name.size());
			SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(type.data()), type.size());
			SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(default_type.data()), default_type.size());
			SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(default_expression.data()), default_expression.size());

			is_first = false;
		}

		SHA512_Final(hash.data(), &ctx);
	}

	using Hash = std::array<unsigned char, SHA512_DIGEST_LENGTH>;

	BlockExtraInfo extra_info;
	std::string names_with_types;
	Hash hash;
	UInt32 structure_class;
};

inline bool operator<(const TableDescription & lhs, const TableDescription & rhs)
{
	return lhs.hash < rhs.hash;
}

using TableDescriptions = std::deque<TableDescription>;

}

InterpreterCheckQuery::InterpreterCheckQuery(DB::ASTPtr query_ptr_, DB::Context& context_)
	: query_ptr(query_ptr_), context(context_)
{
}

Block InterpreterCheckQuery::getSampleBlock() const
{
	Block block;
	ColumnWithTypeAndName col;

	col.name = "status";
	col.type = std::make_shared<DataTypeUInt8>();
	col.column = col.type->createColumn();
	block.insert(col);

	col.name = "host_name";
	col.type = std::make_shared<DataTypeString>();
	col.column = col.type->createColumn();
	block.insert(col);

	col.name = "host_address";
	col.type = std::make_shared<DataTypeString>();
	col.column = col.type->createColumn();
	block.insert(col);

	col.name = "port";
	col.type = std::make_shared<DataTypeUInt16>();
	col.column = col.type->createColumn();
	block.insert(col);

	col.name = "user";
	col.type = std::make_shared<DataTypeString>();
	col.column = col.type->createColumn();
	block.insert(col);

	col.name = "structure_class";
	col.type = std::make_shared<DataTypeUInt32>();
	col.column = col.type->createColumn();
	block.insert(col);

	col.name = "structure";
	col.type = std::make_shared<DataTypeString>();
	col.column = col.type->createColumn();
	block.insert(col);

	return block;
}


BlockIO InterpreterCheckQuery::execute()
{
	ASTCheckQuery & alter = typeid_cast<ASTCheckQuery &>(*query_ptr);
	String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;

	StoragePtr table = context.getTable(database_name, table_name);

	auto distributed_table = typeid_cast<StorageDistributed *>(&*table);
	if (distributed_table != nullptr)
	{
		/// Для таблиц с движком Distributed запрос CHECK TABLE отправляет запрос DESCRIBE TABLE на все реплики.
		/// Проверяется идентичность структур (имена столбцов + типы столбцов + типы по-умолчанию + выражения
		/// по-умолчанию) таблиц, на котороые смотрит распределённая таблица.

		const auto settings = context.getSettings();

		BlockInputStreams streams = distributed_table->describe(context, settings);
		streams[0] = std::make_shared<UnionBlockInputStream<StreamUnionMode::ExtraInfo>>(
			streams, nullptr, settings.max_distributed_connections);
		streams.resize(1);

		auto stream_ptr = dynamic_cast<IProfilingBlockInputStream *>(&*streams[0]);
		if (stream_ptr == nullptr)
			throw Exception("InterpreterCheckQuery: Internal error", ErrorCodes::LOGICAL_ERROR);
		auto & stream = *stream_ptr;

		/// Получить все данные от запросов DESCRIBE TABLE.

		TableDescriptions table_descriptions;

		while (true)
		{
			if (stream.isCancelled())
			{
				BlockIO res;
				res.in = std::make_shared<OneBlockInputStream>(result);
				return res;
			}

			Block block = stream.read();
			if (!block)
				break;

			BlockExtraInfo info = stream.getBlockExtraInfo();
			if (!info.is_valid)
				throw Exception("Received invalid block extra info", ErrorCodes::INVALID_BLOCK_EXTRA_INFO);

			table_descriptions.emplace_back(block, info);
		}

		if (table_descriptions.empty())
			throw Exception("Received empty data", ErrorCodes::RECEIVED_EMPTY_DATA);

		/// Определить класс эквивалентности каждой структуры таблицы.

		std::sort(table_descriptions.begin(), table_descriptions.end());

		UInt32 structure_class = 0;

		auto it = table_descriptions.begin();
		it->structure_class = structure_class;

		auto prev = it;
		for (++it; it != table_descriptions.end(); ++it)
		{
			if (*prev < *it)
				++structure_class;
			it->structure_class = structure_class;
			prev = it;
		}

		/// Составить результат.

		ColumnPtr status_column = std::make_shared<ColumnUInt8>();
		ColumnPtr host_name_column = std::make_shared<ColumnString>();
		ColumnPtr host_address_column = std::make_shared<ColumnString>();
		ColumnPtr port_column = std::make_shared<ColumnUInt16>();
		ColumnPtr user_column = std::make_shared<ColumnString>();
		ColumnPtr structure_class_column = std::make_shared<ColumnUInt32>();
		ColumnPtr structure_column = std::make_shared<ColumnString>();

		/// Это значение равно 1, если структура нигде не отлчиается, а 0 в противном случае.
		UInt8 status_value = (structure_class == 0) ? 1 : 0;

		for (const auto & desc : table_descriptions)
		{
			status_column->insert(static_cast<UInt64>(status_value));
			structure_class_column->insert(static_cast<UInt64>(desc.structure_class));
			host_name_column->insert(desc.extra_info.host);
			host_address_column->insert(desc.extra_info.resolved_address);
			port_column->insert(static_cast<UInt64>(desc.extra_info.port));
			user_column->insert(desc.extra_info.user);
			structure_column->insert(desc.names_with_types);
		}

		Block block;

		block.insert(ColumnWithTypeAndName(status_column, std::make_shared<DataTypeUInt8>(), "status"));
		block.insert(ColumnWithTypeAndName(host_name_column, std::make_shared<DataTypeString>(), "host_name"));
		block.insert(ColumnWithTypeAndName(host_address_column, std::make_shared<DataTypeString>(), "host_address"));
		block.insert(ColumnWithTypeAndName(port_column, std::make_shared<DataTypeUInt16>(), "port"));
		block.insert(ColumnWithTypeAndName(user_column, std::make_shared<DataTypeString>(), "user"));
		block.insert(ColumnWithTypeAndName(structure_class_column, std::make_shared<DataTypeUInt32>(), "structure_class"));
		block.insert(ColumnWithTypeAndName(structure_column, std::make_shared<DataTypeString>(), "structure"));

		BlockIO res;
		res.in = std::make_shared<OneBlockInputStream>(block);
		res.in_sample = getSampleBlock();

		return res;
	}
	else
	{
		result = Block{{ std::make_shared<ColumnUInt8>(), std::make_shared<DataTypeUInt8>(), "result" }};
		result.getByPosition(0).column->insert(Field(UInt64(table->checkData())));

		BlockIO res;
		res.in = std::make_shared<OneBlockInputStream>(result);
		res.in_sample = result.cloneEmpty();

		return res;
	}
}

}
