#pragma once

#include <DB/Core/Block.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
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
	MongoDBBlockInputStream(
		std::unique_ptr<mongo::DBClientCursor> cursor_, const Block & sample_block_, const std::size_t max_block_size)
		: cursor{std::move(cursor_)}, sample_block{sample_block_}, max_block_size{max_block_size}
	{
		/// do nothing if cursor has no data
		if (!cursor->more())
			return;

		const auto num_columns = sample_block.columns();
		types.reserve(num_columns);
		names.reserve(num_columns);
		sample_columns.reserve(num_columns);

		/// save types of each column to eliminate subsequent typeid_cast<> invocations
		for (const auto idx : ext::range(0, num_columns))
		{
			const auto & column = sample_block.getByPosition(idx);
			const auto type = column.type.get();

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
			else if (typeid_cast<const DataTypeFloat64 *>(type))
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

			names.emplace_back(column.name);
			sample_columns.emplace_back(column.column.get());
		}
	}

	String getName() const override { return "MongoDB"; }

	String getID() const override
	{
		using stream = std::ostringstream;

		return "MongoDB(@" + static_cast<stream &>(stream{} << cursor.get()).str() + ")";
	}

private:
	Block readImpl() override
	{
		/// return an empty block if cursor has no data
		if (!cursor->more())
			return {};

		auto block = sample_block.cloneEmpty();

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
				const auto & name = names[idx];
				const auto value = row[name];
				if (value.ok())
					insertValue(columns[idx], types[idx], value, name);
				else
					insertDefaultValue(columns[idx], *sample_columns[idx]);
			}

			++num_rows;
			if (num_rows == max_block_size)
				break;
		}

		return block;
	}

	static void insertValue(
		IColumn * const column, const value_type_t type, const mongo::BSONElement & value, const mongo::StringData & name)
	{
		switch (type)
		{
			case value_type_t::UInt8:
			{
				if (!value.isNumber() && value.type() != mongo::Bool)
					throw Exception{
						"Type mismatch, expected a number or Bool, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnUInt8 *>(column)->insert(value.isNumber() ? value.numberInt() : value.boolean());
				break;
			}
			case value_type_t::UInt16:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnUInt16 *>(column)->insert(value.numberInt());
				break;
			}
			case value_type_t::UInt32:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnUInt32 *>(column)->insert(value.numberLong());
				break;
			}
			case value_type_t::UInt64:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnUInt64 *>(column)->insert(value.numberLong());
				break;
			}
			case value_type_t::Int8:
			{
				if (!value.isNumber() && value.type() != mongo::Bool)
					throw Exception{
						"Type mismatch, expected a number or Bool, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnInt8 *>(column)->insert(value.isNumber() ? value.numberInt() : value.numberInt());
				break;
			}
			case value_type_t::Int16:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnInt16 *>(column)->insert(value.numberInt());
				break;
			}
			case value_type_t::Int32:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnInt32 *>(column)->insert(value.numberInt());
				break;
			}
			case value_type_t::Int64:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnInt64 *>(column)->insert(value.numberLong());
				break;
			}
			case value_type_t::Float32:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnFloat32 *>(column)->insert(value.number());
				break;
			}
			case value_type_t::Float64:
			{
				if (!value.isNumber())
					throw Exception{
						"Type mismatch, expected a number, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnFloat64 *>(column)->insert(value.number());
				break;
			}
			case value_type_t::String:
			{
				if (value.type() != mongo::String)
					throw Exception{
						"Type mismatch, expected String, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				const auto string = value.String();
				static_cast<ColumnString *>(column)->insertDataWithTerminatingZero(string.data(), string.size() + 1);
				break;
			}
			case value_type_t::Date:
			{
				if (value.type() != mongo::Date)
					throw Exception{
						"Type mismatch, expected Date, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

				static_cast<ColumnUInt16 *>(column)->insert(
					UInt16{DateLUT::instance().toDayNum(value.date().toTimeT())});
				break;
			}
			case value_type_t::DateTime:
			{
				if (value.type() != mongo::Date)
					throw Exception{
						"Type mismatch, expected Date, got " + std::string{mongo::typeName(value.type())} +
							" for column " + name.toString(),
						ErrorCodes::TYPE_MISMATCH
					};

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
	Block sample_block;
	const std::size_t max_block_size;
	std::vector<value_type_t> types;
	std::vector<mongo::StringData> names;
	std::vector<const IColumn *> sample_columns;
};

}
