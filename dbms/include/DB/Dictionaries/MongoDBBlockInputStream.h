#pragma once

#include <DB/Core/Block.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Dictionaries/ExternalResultDescription.h>
#include <ext/range.hpp>
#include <mongo/client/dbclient.h>
#include <vector>
#include <string>
#include <DB/Core/FieldVisitors.h>


namespace DB
{

/// Converts mongo::DBClientCursor to a stream of DB::Block`s
class MongoDBBlockInputStream final : public IProfilingBlockInputStream
{
public:
	MongoDBBlockInputStream(
		std::unique_ptr<mongo::DBClientCursor> cursor_, const Block & sample_block, const std::size_t max_block_size)
		: cursor{std::move(cursor_)}, max_block_size{max_block_size}
	{
		/// do nothing if cursor has no data
		if (!cursor->more())
			return;

		description.init(sample_block);
	}

	String getName() const override { return "MongoDB"; }

	String getID() const override
	{
		using stream = std::ostringstream;
		return "MongoDB(@" + static_cast<stream &>(stream{} << cursor.get()).str() + ")";
	}

private:
	using value_type_t = ExternalResultDescription::value_type_t;

	Block readImpl() override
	{
		/// return an empty block if cursor has no data
		if (!cursor->more())
			return {};

		auto block = description.sample_block.cloneEmpty();

		/// cache pointers returned by the calls to getByPosition
		std::vector<IColumn *> columns(block.columns());
		const auto size = columns.size();

		for (const auto i : ext::range(0, size))
			columns[i] = block.getByPosition(i).column.get();

		std::size_t num_rows = 0;
		while (cursor->more())
		{
			const auto row = cursor->next();

			for (const auto idx : ext::range(0, size))
			{
				const auto & name = description.names[idx];
				const auto value = row[name];
				if (value.ok())
					insertValue(columns[idx], description.types[idx], value, name);
				else
					insertDefaultValue(columns[idx], *description.sample_columns[idx]);
			}

			++num_rows;
			if (num_rows == max_block_size)
				break;
		}

		return block;
	}

	static void insertValue(
		IColumn * const column, const value_type_t type, const mongo::BSONElement & value, const std::string & name)
	{
		switch (type)
		{
			case value_type_t::UInt8:
			{
				if (!value.isNumber() && value.type() != mongo::Bool)
					throw Exception{
						"Type mismatch, expected a number or Bool, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnUInt8 *>(column)->insert(value.isNumber() ? value.numberInt() : value.boolean());
				break;
			}
			case value_type_t::UInt16:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnUInt16 *>(column)->insert(value.numberInt());
				break;
			}
			case value_type_t::UInt32:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnUInt32 *>(column)->insert(value.numberLong());
				break;
			}
			case value_type_t::UInt64:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnUInt64 *>(column)->insert(value.numberLong());
				break;
			}
			case value_type_t::Int8:
			{
				if (!value.isNumber() && value.type() != mongo::Bool)
					throw Exception{
						"Type mismatch, expected a number or Bool, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnInt8 *>(column)->insert(value.isNumber() ? value.numberInt() : value.numberInt());
				break;
			}
			case value_type_t::Int16:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnInt16 *>(column)->insert(value.numberInt());
				break;
			}
			case value_type_t::Int32:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnInt32 *>(column)->insert(value.numberInt());
				break;
			}
			case value_type_t::Int64:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnInt64 *>(column)->insert(value.numberLong());
				break;
			}
			case value_type_t::Float32:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnFloat32 *>(column)->insert(value.number());
				break;
			}
			case value_type_t::Float64:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnFloat64 *>(column)->insert(value.number());
				break;
			}
			case value_type_t::String:
			{
				if (value.type() != mongo::String)
					throw Exception{
						"Type mismatch, expected String, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				const auto string = value.String();
				static_cast<ColumnString *>(column)->insertDataWithTerminatingZero(string.data(), string.size() + 1);
				break;
			}
			case value_type_t::Date:
			{
				if (value.type() != mongo::Date)
					throw Exception{
						"Type mismatch, expected Date, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnUInt16 *>(column)->insert(
					UInt16{DateLUT::instance().toDayNum(value.date().toTimeT())});
				break;
			}
			case value_type_t::DateTime:
			{
				if (value.type() != mongo::Date)
					throw Exception{
						"Type mismatch, expected Date, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name, ErrorCodes::TYPE_MISMATCH};

				static_cast<ColumnUInt32 *>(column)->insert(value.date().toTimeT());
				break;
			}
		}
	}

	static void insertDefaultValue(IColumn * const column, const IColumn & sample_column)
	{
		column->insertFrom(sample_column, 0);
	}

	std::unique_ptr<mongo::DBClientCursor> cursor;
	const std::size_t max_block_size;
	ExternalResultDescription description;
};

}
