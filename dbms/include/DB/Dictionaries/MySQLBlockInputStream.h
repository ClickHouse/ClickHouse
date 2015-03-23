#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <statdaemons/ext/range.hpp>
#include <mysqlxx/Query.h>
#include <vector>
#include <string>

namespace DB
{

/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream final : public IProfilingBlockInputStream
{
	enum struct value_type_t
	{
		UInt8,
		UInt16,
		UInt32,
		UInt64,
		Int8,
		Int16,
		Int32,
		Int64,
		Float32,
		Float64,
		String,
		Date,
		DateTime
	};

public:
	MySQLBlockInputStream(const mysqlxx::PoolWithFailover::Entry & entry,
		const std::string & query_str,
		const Block & sample_block,
		const std::size_t max_block_size)
		: entry{entry}, query{this->entry->query(query_str)}, result{query.use()},
		  sample_block{sample_block}, max_block_size{max_block_size}
	{
		types.reserve(sample_block.columns());

		for (const auto idx : ext::range(0, sample_block.columns()))
		{
			const auto type = sample_block.getByPosition(idx).type.get();
			if (typeid_cast<const DataTypeUInt8 *>(type))
				types.push_back(value_type_t::UInt8);
			else if (typeid_cast<const DataTypeUInt16 *>(type))
				types.push_back(value_type_t::UInt16);
			else if (typeid_cast<const DataTypeUInt32 *>(type))
				types.push_back(value_type_t::UInt32);
			else if (typeid_cast<const DataTypeUInt64 *>(type))
				types.push_back(value_type_t::UInt64);
			else if (typeid_cast<const DataTypeInt8 *>(type))
				types.push_back(value_type_t::Int8);
			else if (typeid_cast<const DataTypeInt16 *>(type))
				types.push_back(value_type_t::Int16);
			else if (typeid_cast<const DataTypeInt32 *>(type))
				types.push_back(value_type_t::Int32);
			else if (typeid_cast<const DataTypeInt64 *>(type))
				types.push_back(value_type_t::Int64);
			else if (typeid_cast<const DataTypeFloat32 *>(type))
				types.push_back(value_type_t::Float32);
			else if (typeid_cast<const DataTypeInt64 *>(type))
				types.push_back(value_type_t::Float64);
			else if (typeid_cast<const DataTypeString *>(type))
				types.push_back(value_type_t::String);
			else if (typeid_cast<const DataTypeDate *>(type))
				types.push_back(value_type_t::Date);
			else if (typeid_cast<const DataTypeDateTime *>(type))
				types.push_back(value_type_t::DateTime);
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
			/// @todo cache pointers returned by the calls to getByPosition
			for (const auto idx : ext::range(0, row.size()))
				insertValue(block.getByPosition(idx).column, row[idx], types[idx]);

			++rows;
			if (rows == max_block_size)
				break;
		}

		return rows == 0 ? Block{} : block;
	};

	static void insertValue(ColumnPtr & column, const mysqlxx::Value & value, const value_type_t type)
	{
		switch (type)
		{
			case value_type_t::UInt8: column->insert(static_cast<UInt64>(value)); break;
			case value_type_t::UInt16: column->insert(static_cast<UInt64>(value)); break;
			case value_type_t::UInt32: column->insert(static_cast<UInt64>(value)); break;
			case value_type_t::UInt64: column->insert(static_cast<UInt64>(value)); break;
			case value_type_t::Int8: column->insert(static_cast<Int64>(value)); break;
			case value_type_t::Int16: column->insert(static_cast<Int64>(value)); break;
			case value_type_t::Int32: column->insert(static_cast<Int64>(value)); break;
			case value_type_t::Int64: column->insert(static_cast<Int64>(value)); break;
			case value_type_t::Float32: column->insert(static_cast<Float64>(value)); break;
			case value_type_t::Float64: column->insert(static_cast<Float64>(value)); break;
			case value_type_t::String: column->insert(value.getString()); break;
			case value_type_t::Date: column->insert(static_cast<UInt64>(UInt16{value.getDate().getDayNum()})); break;
			case value_type_t::DateTime: column->insert(static_cast<UInt64>(time_t{value.getDateTime()})); break;
		};
	}

	mysqlxx::PoolWithFailover::Entry entry;
	mysqlxx::Query query;
	mysqlxx::UseQueryResult result;
	Block sample_block;
	const std::size_t max_block_size;
	std::vector<value_type_t> types;
};

}
