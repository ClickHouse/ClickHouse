#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Dictionaries/ExternalResultDescription.h>

#include <Poco/Data/Session.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/RecordSet.h>

#include <ext/range.hpp>
#include <vector>
#include <string>


namespace DB
{

namespace ErrorCodes
{
	extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCBlockInputStream final : public IProfilingBlockInputStream
{
public:
	ODBCBlockInputStream(
		Poco::Data::Session && session, const std::string & query_str, const Block & sample_block,
		const std::size_t max_block_size)
		:
		session{session},
		statement{(this->session << query_str, Poco::Data::Keywords::now)},
		result{statement},
		iterator{result.begin()},
		max_block_size{max_block_size}
	{
		if (sample_block.columns() != result.columnCount())
			throw Exception{
				"RecordSet contains " + toString(result.columnCount()) + " columns while " +
					toString(sample_block.columns()) + " expected",
				ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH};

		description.init(sample_block);
	}

	String getName() const override { return "ODBC"; }

	String getID() const override
	{
		return "ODBC(" + statement.toString() + ")";
	}

private:
	using value_type_t = ExternalResultDescription::value_type_t;


	Block readImpl() override
	{
		if (iterator == result.end())
			return {};

		auto block = description.sample_block.cloneEmpty();

		/// cache pointers returned by the calls to getByPosition
		std::vector<IColumn *> columns(block.columns());
		for (const auto i : ext::range(0, columns.size()))
			columns[i] = block.getByPosition(i).column.get();

		std::size_t num_rows = 0;
		while (iterator != result.end())
		{
			Poco::Data::Row & row = *iterator;

			for (const auto idx : ext::range(0, row.fieldCount()))
			{
				const Poco::Dynamic::Var & value = row[idx];

				if (!value.isEmpty())
					insertValue(columns[idx], description.types[idx], value);
				else
					insertDefaultValue(columns[idx], *description.sample_columns[idx]);
			}

			++num_rows;
			if (num_rows == max_block_size)
				break;

			++iterator;
		}

		return block;
	}

	static void insertValue(IColumn * const column, const value_type_t type, const Poco::Dynamic::Var & value)
	{
		switch (type)
		{
			case value_type_t::UInt8: static_cast<ColumnUInt8 *>(column)->insert(value.convert<UInt64>()); break;
			case value_type_t::UInt16: static_cast<ColumnUInt16 *>(column)->insert(value.convert<UInt64>()); break;
			case value_type_t::UInt32: static_cast<ColumnUInt32 *>(column)->insert(value.convert<UInt64>()); break;
			case value_type_t::UInt64: static_cast<ColumnUInt64 *>(column)->insert(value.convert<UInt64>()); break;
			case value_type_t::Int8: static_cast<ColumnInt8 *>(column)->insert(value.convert<Int64>()); break;
			case value_type_t::Int16: static_cast<ColumnInt16 *>(column)->insert(value.convert<Int64>()); break;
			case value_type_t::Int32: static_cast<ColumnInt32 *>(column)->insert(value.convert<Int64>()); break;
			case value_type_t::Int64: static_cast<ColumnInt64 *>(column)->insert(value.convert<Int64>()); break;
			case value_type_t::Float32: static_cast<ColumnFloat32 *>(column)->insert(value.convert<Float64>()); break;
			case value_type_t::Float64: static_cast<ColumnFloat64 *>(column)->insert(value.convert<Float64>()); break;
			case value_type_t::String: static_cast<ColumnString *>(column)->insert(value.convert<String>()); break;
			case value_type_t::Date: static_cast<ColumnUInt16 *>(column)->insert(UInt16{LocalDate{value.convert<String>()}.getDayNum()}); break;
			case value_type_t::DateTime: static_cast<ColumnUInt32 *>(column)->insert(time_t{LocalDateTime{value.convert<String>()}}); break;
		}
	}

	static void insertDefaultValue(IColumn * const column, const IColumn & sample_column)
	{
		column->insertFrom(sample_column, 0);
	}

	Poco::Data::Session session;
	Poco::Data::Statement statement;
	Poco::Data::RecordSet result;
	Poco::Data::RecordSet::Iterator iterator;

	const std::size_t max_block_size;
	ExternalResultDescription description;
};

}
