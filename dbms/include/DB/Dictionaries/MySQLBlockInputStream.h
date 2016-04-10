#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Dictionaries/ExternalResultDescription.h>
#include <ext/range.hpp>
#include <mysqlxx/Query.h>
#include <mysqlxx/PoolWithFailover.h>
#include <vector>
#include <string>

namespace DB
{

namespace ErrorCodes
{
	extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream final : public IProfilingBlockInputStream
{
public:
	MySQLBlockInputStream(
		const mysqlxx::PoolWithFailover::Entry & entry, const std::string & query_str, const Block & sample_block,
		const std::size_t max_block_size)
		: entry{entry}, query{this->entry->query(query_str)}, result{query.use()},
		  max_block_size{max_block_size}
	{
		if (sample_block.columns() != result.getNumFields())
			throw Exception{
				"mysqlxx::UseQueryResult contains " + toString(result.getNumFields()) + " columns while " +
					toString(sample_block.columns()) + " expected",
				ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

		description.init(sample_block);
	}

	String getName() const override { return "MySQL"; }

	String getID() const override
	{
		return "MySQL(" + query.str() + ")";
	}

private:
	using value_type_t = ExternalResultDescription::value_type_t;

	Block readImpl() override
	{
		auto row = result.fetch();
		if (!row)
			return {};

		auto block = description.sample_block.cloneEmpty();

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
					insertValue(columns[idx], description.types[idx], value);
				else
					insertDefaultValue(columns[idx], *description.sample_columns[idx]);
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
			case value_type_t::String: static_cast<ColumnString *>(column)->insertData(value.data(), value.size()); break;
			case value_type_t::Date: static_cast<ColumnUInt16 *>(column)->insert(UInt16{value.getDate().getDayNum()}); break;
			case value_type_t::DateTime: static_cast<ColumnUInt32 *>(column)->insert(time_t{value.getDateTime()}); break;
		}
	}

	static void insertDefaultValue(IColumn * const column, const IColumn & sample_column)
	{
		column->insertFrom(sample_column, 0);
	}

	mysqlxx::PoolWithFailover::Entry entry;
	mysqlxx::Query query;
	mysqlxx::UseQueryResult result;
	const std::size_t max_block_size;
	ExternalResultDescription description;
};

}
