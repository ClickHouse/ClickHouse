#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <statdaemons/ext/range.hpp>
#include <mysqlxx/Query.h>
#include <vector>
#include <string>

namespace DB
{

/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream final : public IProfilingBlockInputStream
{
public:
	MySQLBlockInputStream(mysqlxx::Query query, const Block & sample_block, const std::size_t max_block_size)
		: query{std::move(query)}, result{query.use()}, sample_block{sample_block}, max_block_size{max_block_size}
	{
		types.reserve(sample_block.columns());

		for (const auto idx : ext::range(0, sample_block.columns()))
		{
			const auto type = sample_block.getByPosition(idx).type.get();
			if (typeid_cast<const DataTypeUInt8 *>(type))
				types.push_back(AttributeType::uint8);
			else if (typeid_cast<const DataTypeUInt16 *>(type))
				types.push_back(AttributeType::uint16);
			else if (typeid_cast<const DataTypeUInt32 *>(type))
				types.push_back(AttributeType::uint32);
			else if (typeid_cast<const DataTypeUInt64 *>(type))
				types.push_back(AttributeType::uint64);
			else if (typeid_cast<const DataTypeInt8 *>(type))
				types.push_back(AttributeType::int8);
			else if (typeid_cast<const DataTypeInt16 *>(type))
				types.push_back(AttributeType::int16);
			else if (typeid_cast<const DataTypeInt32 *>(type))
				types.push_back(AttributeType::int32);
			else if (typeid_cast<const DataTypeInt64 *>(type))
				types.push_back(AttributeType::int64);
			else if (typeid_cast<const DataTypeFloat32 *>(type))
				types.push_back(AttributeType::float32);
			else if (typeid_cast<const DataTypeInt64 *>(type))
				types.push_back(AttributeType::float64);
			else if (typeid_cast<const DataTypeString *>(type))
				types.push_back(AttributeType::string);
			else
				throw Exception{
					"Unsupported type " + type->getName(),
					ErrorCodes::UNKNOWN_TYPE
				};
		}
	}

	String getName() const override { return "MySQLBlockInputStream"; }

	String getID() const override
	{
		return "MySQL(" + query.str() + ")";
	}

private:
	Block readImpl() override
	{
		auto block = sample_block.cloneEmpty();

		if (block.columns() != result.getNumFields())
			throw Exception{
				"mysqlxx::UseQueryResult contains " + toString(result.getNumFields()) + " columns while " +
					toString(block.columns()) + " expected",
				ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH
			};

		std::size_t rows = 0;
		while (auto row = result.fetch())
		{
			for (const auto idx : ext::range(0, row.size()))
				insertValue(block.getByPosition(idx).column, row[idx], types[idx]);

			++rows;
			if (rows == max_block_size)
				break;
		}

		return rows == 0 ? Block{} : block;
	};

	static void insertValue(ColumnPtr & column, const mysqlxx::Value & value, const AttributeType type)
	{
		switch (type)
		{
			case AttributeType::uint8: column->insert(static_cast<UInt64>(value)); break;
			case AttributeType::uint16: column->insert(static_cast<UInt64>(value)); break;
			case AttributeType::uint32: column->insert(static_cast<UInt64>(value)); break;
			case AttributeType::uint64: column->insert(static_cast<UInt64>(value)); break;
			case AttributeType::int8: column->insert(static_cast<Int64>(value)); break;
			case AttributeType::int16: column->insert(static_cast<Int64>(value)); break;
			case AttributeType::int32: column->insert(static_cast<Int64>(value)); break;
			case AttributeType::int64: column->insert(static_cast<Int64>(value)); break;
			case AttributeType::float32: column->insert(static_cast<Float64>(value)); break;
			case AttributeType::float64: column->insert(static_cast<Float64>(value)); break;
			case AttributeType::string: column->insert(value.getString()); break;
		}
	}

	mysqlxx::Query query;
	mysqlxx::UseQueryResult result;
	Block sample_block;
	const std::size_t max_block_size;
	std::vector<AttributeType> types;
};

}
