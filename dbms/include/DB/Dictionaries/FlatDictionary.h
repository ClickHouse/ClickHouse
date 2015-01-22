#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Core/Block.h>
#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <statdaemons/ext/range.hpp>
#include <Poco/Util/XMLConfiguration.h>
#include <map>
#include <vector>

namespace DB
{

const auto max_array_size = 500000;
const auto max_block_size = 8192;

/// @todo manage arrays using std::vector or PODArray, start with an initial size, expand up to max_array_size
class FlatDictionary : public IDictionary
{
public:
    FlatDictionary(const DictionaryStructure & dict_struct, const Poco::Util::XMLConfiguration & config,
		const std::string & config_prefix, const Context & context)
	{
		for (const auto & attribute : dict_struct.attributes)
		{
			attributes.emplace(attribute.name,
				createAttributeWithType(getAttributeTypeByName(attribute.type), attribute.null_value));

			if (attribute.hierarchical)
				hierarchical_attribute = &attributes[attribute.name];
		}

		if (config.has(config_prefix + "source.file"))
		{
			const auto & file_name = config.getString(config_prefix + "source.file.path");
			const auto & format = config.getString(config_prefix + "source.file.format");

			ReadBufferFromFile in{file_name};
			auto sample_block = createSampleBlock(dict_struct, context);
			auto stream = context.getFormatFactory().getInput(
				format, in, sample_block, max_block_size, context.getDataTypeFactory());

			while (const auto block = stream->read())
			{
				const auto & id_column = *block.getByPosition(0).column;

				for (const auto attribute_idx : ext::range(1, attributes.size()))
				{
					const auto & attribute_column = *block.getByPosition(attribute_idx).column;
					auto & attribute = attributes[dict_struct.attributes[attribute_idx - 1].name];

					for (const auto row_idx : ext::range(1, attribute_column.size()))
						setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]);
				}
			}
		}
	}

	UInt64 getUInt64(const id_t id, const std::string & attribute_name) const override
	{
		const auto & attribute = findAttribute(attribute_name);

		if (attribute.type != attribute_type::uint64)
			throw Exception{
				"Type mismatch: attribute " + attribute_name + " has a type different from UInt64",
				ErrorCodes::TYPE_MISMATCH
			};

		if (id < max_array_size)
			return attribute.uint64_array[id];

		return attribute.uint64_null_value;
	}

    StringRef getString(const id_t id, const std::string & attribute_name) const override
	{
		const auto & attribute = findAttribute(attribute_name);

		if (attribute.type != attribute_type::string)
			throw Exception{
				"Type mismatch: attribute " + attribute_name + " has a type different from String",
				ErrorCodes::TYPE_MISMATCH
			};

		if (id < max_array_size)
			return { attribute.string_array[id].data(), attribute.string_array[id].size() };

        return { attribute.string_null_value.data(), attribute.string_null_value.size() };
    }

private:
	enum class attribute_type
	{
		uint8,
		uint16,
		uint32,
		uint64,
		int8,
		int16,
		int32,
		int64,
		string
	};

	struct attribute_t
	{
		attribute_type type;
		UInt8 uint8_null_value;
		UInt16 uint16_null_value;
		UInt32 uint32_null_value;
		UInt64 uint64_null_value;
		Int8 int8_null_value;
		Int16 int16_null_value;
		Int32 int32_null_value;
		Int64 int64_null_value;
		String string_null_value;
		std::unique_ptr<UInt8[]> uint8_array;
		std::unique_ptr<UInt16[]> uint16_array;
		std::unique_ptr<UInt32[]> uint32_array;
		std::unique_ptr<UInt64[]> uint64_array;
		std::unique_ptr<Int8[]> int8_array;
		std::unique_ptr<Int16[]> int16_array;
		std::unique_ptr<Int32[]> int32_array;
		std::unique_ptr<Int64[]> int64_array;
		std::unique_ptr<String[]> string_array;
	};

	using attributes_t = std::map<std::string, attribute_t>;

	attribute_t createAttributeWithType(const attribute_type type, const std::string & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case attribute_type::uint8:
				attr.uint8_null_value = DB::parse<UInt8>(null_value);
				attr.uint8_array.reset(new UInt8[max_array_size]);
				std::fill(attr.uint8_array.get(), attr.uint8_array.get() + max_array_size, attr.uint8_null_value);
				break;
			case attribute_type::uint16:
				attr.uint16_null_value = DB::parse<UInt16>(null_value);
				attr.uint16_array.reset(new UInt16[max_array_size]);
				std::fill(attr.uint16_array.get(), attr.uint16_array.get() + max_array_size, attr.uint16_null_value);
				break;
			case attribute_type::uint32:
				attr.uint32_null_value = DB::parse<UInt32>(null_value);
				attr.uint32_array.reset(new UInt32[max_array_size]);
				std::fill(attr.uint32_array.get(), attr.uint32_array.get() + max_array_size, attr.uint32_null_value);
				break;
			case attribute_type::uint64:
				attr.uint64_null_value = DB::parse<UInt64>(null_value);
				attr.uint64_array.reset(new UInt64[max_array_size]);
				std::fill(attr.uint64_array.get(), attr.uint64_array.get() + max_array_size, attr.uint64_null_value);
				break;
			case attribute_type::int8:
				attr.int8_null_value = DB::parse<Int8>(null_value);
				attr.int8_array.reset(new Int8[max_array_size]);
				std::fill(attr.int8_array.get(), attr.int8_array.get() + max_array_size, attr.int8_null_value);
				break;
			case attribute_type::int16:
				attr.int16_null_value = DB::parse<Int16>(null_value);
				attr.int16_array.reset(new Int16[max_array_size]);
				std::fill(attr.int16_array.get(), attr.int16_array.get() + max_array_size, attr.int16_null_value);
				break;
			case attribute_type::int32:
				attr.int32_null_value = DB::parse<Int32>(null_value);
				attr.int32_array.reset(new Int32[max_array_size]);
				std::fill(attr.int32_array.get(), attr.int32_array.get() + max_array_size, attr.int32_null_value);
				break;
			case attribute_type::int64:
				attr.int64_null_value = DB::parse<Int64>(null_value);
				attr.int64_array.reset(new Int64[max_array_size]);
				std::fill(attr.int64_array.get(), attr.int64_array.get() + max_array_size, attr.int64_null_value);
				break;
			case attribute_type::string:
				attr.string_null_value = null_value;
				attr.string_array.reset(new String[max_array_size]);
				std::fill(attr.string_array.get(), attr.string_array.get() + max_array_size, attr.string_null_value);
				break;
		}

		return attr;
	}

	attribute_type getAttributeTypeByName(const std::string & type)
	{
		static const std::unordered_map<std::string, attribute_type> dictionary{
			{ "UInt8", attribute_type::uint8 },
			{ "UInt16", attribute_type::uint16 },
			{ "UInt32", attribute_type::uint32 },
			{ "UInt64", attribute_type::uint64 },
			{ "Int8", attribute_type::int8 },
			{ "Int16", attribute_type::int16 },
			{ "Int32", attribute_type::int32 },
			{ "Int64", attribute_type::int64 },
			{ "String", attribute_type::string },
		};

		const auto it = dictionary.find(type);
		if (it != std::end(dictionary))
			return it->second;

		throw Exception{
			"Unknown type " + type,
			ErrorCodes::UNKNOWN_TYPE
		};
	}

	void setAttributeValue(attribute_t & attribute, const id_t id, const Field & value)
	{
		if (id >= max_array_size)
			throw Exception{
				"Identifier should be less than " + toString(max_array_size),
				ErrorCodes::ARGUMENT_OUT_OF_BOUND
			};

		switch (attribute.type)
		{
			case attribute_type::uint8: attribute.uint8_array[id] = value.get<UInt64>(); break;
			case attribute_type::uint16: attribute.uint16_array[id] = value.get<UInt64>(); break;
			case attribute_type::uint32: attribute.uint32_array[id] = value.get<UInt64>(); break;
			case attribute_type::uint64: attribute.uint64_array[id] = value.get<UInt64>(); break;
			case attribute_type::int8: attribute.int8_array[id] = value.get<Int64>(); break;
			case attribute_type::int16: attribute.int16_array[id] = value.get<Int64>(); break;
			case attribute_type::int32: attribute.int32_array[id] = value.get<Int64>(); break;
			case attribute_type::int64: attribute.int64_array[id] = value.get<Int64>(); break;
			case attribute_type::string: attribute.string_array[id] = value.get<String>(); break;
		}
	}

	static Block createSampleBlock(const DictionaryStructure & dict_struct, const Context & context)
	{
		Block block{
			ColumnWithNameAndType{
				new ColumnUInt64,
				new DataTypeUInt64,
				dict_struct.id_name
			}
		};

		for (const auto & attribute : dict_struct.attributes)
		{
			const auto & type = context.getDataTypeFactory().get(attribute.type);
			block.insert(ColumnWithNameAndType{
				type->createColumn(), type, attribute.name
			});
		}

		return block;
	}

	const attribute_t & findAttribute(const std::string & attribute_name) const
	{
		const auto it = attributes.find(attribute_name);
		if (it == std::end(attributes))
			throw Exception{
				"No such attribute '" + attribute_name + "'",
				ErrorCodes::BAD_ARGUMENTS
			};

		return it->second;
	}

    attributes_t attributes;
	const attribute_t * hierarchical_attribute = nullptr;
};

}
