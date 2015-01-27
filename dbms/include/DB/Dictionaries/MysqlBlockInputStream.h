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

class MysqlBlockInputStream final : public IProfilingBlockInputStream
{
public:
	MysqlBlockInputStream(mysqlxx::Query query, const Block & sample_block, const std::size_t max_block_size)
		: query{std::move(query)}, result{query.use()}, sample_block{sample_block}, max_block_size{max_block_size}
	{
		types.reserve(sample_block.columns());

		for (const auto idx : ext::range(0, sample_block.columns()))
		{
			const auto type = sample_block.getByPosition(idx).type.get();
			if (typeid_cast<const DataTypeUInt8 *>(type))
				types.push_back(attribute_type::uint8);
			else if (typeid_cast<const DataTypeUInt16 *>(type))
				types.push_back(attribute_type::uint16);
			else if (typeid_cast<const DataTypeUInt32 *>(type))
				types.push_back(attribute_type::uint32);
			else if (typeid_cast<const DataTypeUInt64 *>(type))
				types.push_back(attribute_type::uint64);
			else if (typeid_cast<const DataTypeInt8 *>(type))
				types.push_back(attribute_type::int8);
			else if (typeid_cast<const DataTypeInt16 *>(type))
				types.push_back(attribute_type::int16);
			else if (typeid_cast<const DataTypeInt32 *>(type))
				types.push_back(attribute_type::int32);
			else if (typeid_cast<const DataTypeInt64 *>(type))
				types.push_back(attribute_type::int64);
			else if (typeid_cast<const DataTypeFloat32 *>(type))
				types.push_back(attribute_type::float32);
			else if (typeid_cast<const DataTypeInt64 *>(type))
				types.push_back(attribute_type::float64);
			else if (typeid_cast<const DataTypeString *>(type))
				types.push_back(attribute_type::string);
			else
				throw Exception{
					"Unsupported type " + type->getName(),
					ErrorCodes::UNKNOWN_TYPE
				};
		}
	}

	String getName() const override { return "MysqlBlockInputStream"; }

	String getID() const override
	{
		return "Mysql(" + query.str() + ")";
	}

private:
	Block readImpl() override
	{
		auto block = sample_block.cloneEmpty();

		if (block.columns() != result.getNumFields())
			throw Exception{
				"mysqlxx::UserQueryResult contains " + toString(result.getNumFields()) + " columns while " +
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

	static void insertValue(ColumnPtr & column, const mysqlxx::Value & value, const attribute_type type)
	{
		switch (type)
		{
			case attribute_type::uint8: column->insert(static_cast<UInt64>(value)); break;
			case attribute_type::uint16: column->insert(static_cast<UInt64>(value)); break;
			case attribute_type::uint32: column->insert(static_cast<UInt64>(value)); break;
			case attribute_type::uint64: column->insert(static_cast<UInt64>(value)); break;
			case attribute_type::int8: column->insert(static_cast<Int64>(value)); break;
			case attribute_type::int16: column->insert(static_cast<Int64>(value)); break;
			case attribute_type::int32: column->insert(static_cast<Int64>(value)); break;
			case attribute_type::int64: column->insert(static_cast<Int64>(value)); break;
			case attribute_type::float32: column->insert(static_cast<Float64>(value)); break;
			case attribute_type::float64: column->insert(static_cast<Float64>(value)); break;
			case attribute_type::string: column->insert(value.getString()); break;
		}
	}

	mysqlxx::Query query;
	mysqlxx::UseQueryResult result;
	Block sample_block;
	std::size_t max_block_size;
	std::vector<attribute_type> types;
};

}
