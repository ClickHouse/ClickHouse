#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <statdaemons/ext/range.hpp>
#include <mysqlxx/Query.h>
#include <mysqlxx/PoolWithFailover.h>
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
		if (sample_block.columns() != result.getNumFields())
			throw Exception{
				"mysqlxx::UseQueryResult contains " + toString(result.getNumFields()) + " columns while " +
					toString(sample_block.columns()) + " expected",
				ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH
			};

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

	String getName() const override { return "MySQL"; }

	String getID() const override
	{
		return "MySQL(" + query.str() + ")";
	}

private:
	Block readImpl() override
	{
		auto row = result.fetch();
		if (!row)
			return {};

		auto block = sample_block.cloneEmpty();

		/// cache pointers returned by the calls to getByPosition
		std::vector<IColumn *> columns(block.columns());
		for (const auto i : ext::range(0, columns.size()))
			columns[i] = block.getByPosition(i).column.get();

		std::size_t num_rows = 0;
		while (row)
		{
			for (const auto idx : ext::range(0, row.size()))
			{
				const auto value = row[idx];
				if (!value.isNull())
					insertValue(columns[idx], types[idx], value);
				else
					insertDefaultValue(columns[idx], types[idx]);
			}

			++num_rows;
			if (num_rows == max_block_size)
				break;

			row = result.fetch();
		}

		return block;
	}

	static void insertValue(IColumn * const column, const value_type_t type, const mysqlxx::Value & value)
	{
		switch (type)
		{
			case value_type_t::UInt8: static_cast<ColumnUInt8 *>(column)->insert(value.getUInt()); break;
			case value_type_t::UInt16: static_cast<ColumnUInt16 *>(column)->insert(value.getUInt()); break;
			case value_type_t::UInt32: static_cast<ColumnUInt32 *>(column)->insert(value.getUInt()); break;
			case value_type_t::UInt64: static_cast<ColumnUInt64 *>(column)->insert(value.getUInt()); break;
			case value_type_t::Int8: static_cast<ColumnInt8 *>(column)->insert(value.getInt()); break;
			case value_type_t::Int16: static_cast<ColumnInt16 *>(column)->insert(value.getInt()); break;
			case value_type_t::Int32: static_cast<ColumnInt32 *>(column)->insert(value.getInt()); break;
			case value_type_t::Int64: static_cast<ColumnInt64 *>(column)->insert(value.getInt()); break;
			case value_type_t::Float32: static_cast<ColumnFloat32 *>(column)->insert(value.getDouble()); break;
			case value_type_t::Float64: static_cast<ColumnFloat64 *>(column)->insert(value.getDouble()); break;
			case value_type_t::String:
			{
				const auto string = value.getString();
				static_cast<ColumnString *>(column)->insertDataWithTerminatingZero(string.data(), string.size() + 1);
				break;
			}
			case value_type_t::Date: static_cast<ColumnUInt16 *>(column)->insert(UInt16{value.getDate().getDayNum()}); break;
			case value_type_t::DateTime: static_cast<ColumnUInt32 *>(column)->insert(time_t{value.getDateTime()}); break;
		}
	}

	static void insertDefaultValue(IColumn * const column, const value_type_t type)
	{
		switch (type)
		{
			case value_type_t::UInt8: static_cast<ColumnUInt8 *>(column)->insertDefault(); break;
			case value_type_t::UInt16: static_cast<ColumnUInt16 *>(column)->insertDefault(); break;
			case value_type_t::UInt32: static_cast<ColumnUInt32 *>(column)->insertDefault(); break;
			case value_type_t::UInt64: static_cast<ColumnUInt64 *>(column)->insertDefault(); break;
			case value_type_t::Int8: static_cast<ColumnInt8 *>(column)->insertDefault(); break;
			case value_type_t::Int16: static_cast<ColumnInt16 *>(column)->insertDefault(); break;
			case value_type_t::Int32: static_cast<ColumnInt32 *>(column)->insertDefault(); break;
			case value_type_t::Int64: static_cast<ColumnInt64 *>(column)->insertDefault(); break;
			case value_type_t::Float32: static_cast<ColumnFloat32 *>(column)->insertDefault(); break;
			case value_type_t::Float64: static_cast<ColumnFloat64 *>(column)->insertDefault(); break;
			case value_type_t::String: static_cast<ColumnString *>(column)->insertDefault(); break;
			case value_type_t::Date: static_cast<ColumnUInt16 *>(column)->insertDefault(); break;
			case value_type_t::DateTime: static_cast<ColumnUInt32 *>(column)->insertDefault(); break;
		}
	}

	mysqlxx::PoolWithFailover::Entry entry;
	mysqlxx::Query query;
	mysqlxx::UseQueryResult result;
	Block sample_block;
	const std::size_t max_block_size;
	std::vector<value_type_t> types;
};

}
