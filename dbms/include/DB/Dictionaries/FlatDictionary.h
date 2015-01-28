#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/IDictionary.h>
#include <statdaemons/ext/range.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <vector>

namespace DB
{

const auto initial_array_size = 1024;
const auto max_array_size = 500000;

class FlatDictionary final : public IDictionary
{
public:
    FlatDictionary(const DictionaryStructure & dict_struct, const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix, DictionarySourcePtr source_ptr)
	: source_ptr{std::move(source_ptr)}
	{
		const auto size = dict_struct.attributes.size();
		attributes.resize(size);
		for (const auto idx : ext::range(0, size))
		{
			const auto & attribute = dict_struct.attributes[idx];
			attribute_index_by_name.emplace(attribute.name, idx);
			attributes[idx] = std::move(createAttributeWithType(getAttributeTypeByName(attribute.type),
				attribute.null_value));

			if (attribute.hierarchical)
				hierarchical_attribute = &attributes[idx];
		}

		auto stream = this->source_ptr->loadAll();

		while (const auto block = stream->read())
		{
			const auto & id_column = *block.getByPosition(0).column;

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *block.getByPosition(attribute_idx + 1).column;
				auto & attribute = attributes[attribute_idx];

				for (const auto row_idx : ext::range(0, id_column.size()))
					setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]);
			}
		}
	}

	id_t toParent(const id_t id) const override
	{
		const auto attr = hierarchical_attribute;

		switch (hierarchical_attribute->type)
		{
		case attribute_type::uint8: return id < attr->uint8_array->size() ? (*attr->uint8_array)[id] : attr->uint8_null_value;
		case attribute_type::uint16: return id < attr->uint16_array->size() ? (*attr->uint16_array)[id] : attr->uint16_null_value;
		case attribute_type::uint32: return id < attr->uint32_array->size() ? (*attr->uint32_array)[id] : attr->uint32_null_value;
		case attribute_type::uint64: return id < attr->uint64_array->size() ? (*attr->uint64_array)[id] : attr->uint64_null_value;
		case attribute_type::int8: return id < attr->int8_array->size() ? (*attr->int8_array)[id] : attr->int8_null_value;
		case attribute_type::int16: return id < attr->int16_array->size() ? (*attr->int16_array)[id] : attr->int16_null_value;
		case attribute_type::int32: return id < attr->int32_array->size() ? (*attr->int32_array)[id] : attr->int32_null_value;
		case attribute_type::int64: return id < attr->int64_array->size() ? (*attr->int64_array)[id] : attr->int64_null_value;
		case attribute_type::float32:
		case attribute_type::float64:
		case attribute_type::string:
			break;
		}

		throw Exception{
			"Hierarchical attribute has non-integer type " + toString(hierarchical_attribute->type),
			ErrorCodes::TYPE_MISMATCH
		};
	}

	#define DECLARE_SAFE_GETTER(TYPE, NAME, LC_TYPE) \
	TYPE get##NAME(const std::string & attribute_name, const id_t id) const override\
	{\
		const auto idx = getAttributeIndex(attribute_name);\
		const auto & attribute = attributes[idx];\
		if (attribute.type != attribute_type::LC_TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		if (id < attribute.LC_TYPE##_array->size())\
			return (*attribute.LC_TYPE##_array)[id];\
		return attribute.LC_TYPE##_null_value;\
	}
	DECLARE_SAFE_GETTER(UInt8, UInt8, uint8)
	DECLARE_SAFE_GETTER(UInt16, UInt16, uint16)
	DECLARE_SAFE_GETTER(UInt32, UInt32, uint32)
	DECLARE_SAFE_GETTER(UInt64, UInt64, uint64)
	DECLARE_SAFE_GETTER(Int8, Int8, int8)
	DECLARE_SAFE_GETTER(Int16, Int16, int16)
	DECLARE_SAFE_GETTER(Int32, Int32, int32)
	DECLARE_SAFE_GETTER(Int64, Int64, int64)
	DECLARE_SAFE_GETTER(Float32, Float32, float32)
	DECLARE_SAFE_GETTER(Float64, Float64, float64)
	DECLARE_SAFE_GETTER(StringRef, String, string)
	#undef DECLARE_SAFE_GETTER

	std::size_t getAttributeIndex(const std::string & attribute_name) const override
	{
		const auto it = attribute_index_by_name.find(attribute_name);
		if (it == std::end(attribute_index_by_name))
			throw Exception{
				"No such attribute '" + attribute_name + "'",
				ErrorCodes::BAD_ARGUMENTS
			};

		return it->second;
	}

	#define DECLARE_TYPE_CHECKER(NAME, LC_NAME)\
	bool is##NAME(const std::size_t attribute_idx) const override\
	{\
		return attributes[attribute_idx].type == attribute_type::LC_NAME;\
	}
	DECLARE_TYPE_CHECKER(UInt8, uint8)
	DECLARE_TYPE_CHECKER(UInt16, uint16)
	DECLARE_TYPE_CHECKER(UInt32, uint32)
	DECLARE_TYPE_CHECKER(UInt64, uint64)
	DECLARE_TYPE_CHECKER(Int8, int8)
	DECLARE_TYPE_CHECKER(Int16, int16)
	DECLARE_TYPE_CHECKER(Int32, int32)
	DECLARE_TYPE_CHECKER(Int64, int64)
	DECLARE_TYPE_CHECKER(Float32, float32)
	DECLARE_TYPE_CHECKER(Float64, float64)
	DECLARE_TYPE_CHECKER(String, string)
	#undef DECLARE_TYPE_CHECKER

	#define DECLARE_UNSAFE_GETTER(TYPE, NAME, LC_NAME)\
	TYPE get##NAME##Unsafe(const std::size_t attribute_idx, const id_t id) const override\
	{\
		const auto & attribute = attributes[attribute_idx];\
		return id < attribute.LC_NAME##_array->size() ? (*attribute.LC_NAME##_array)[id] : attribute.LC_NAME##_null_value;\
	}
	DECLARE_UNSAFE_GETTER(UInt8, UInt8, uint8)
	DECLARE_UNSAFE_GETTER(UInt16, UInt16, uint16)
	DECLARE_UNSAFE_GETTER(UInt32, UInt32, uint32)
	DECLARE_UNSAFE_GETTER(UInt64, UInt64, uint64)
	DECLARE_UNSAFE_GETTER(Int8, Int8, int8)
	DECLARE_UNSAFE_GETTER(Int16, Int16, int16)
	DECLARE_UNSAFE_GETTER(Int32, Int32, int32)
	DECLARE_UNSAFE_GETTER(Int64, Int64, int64)
	DECLARE_UNSAFE_GETTER(Float32, Float32, float32)
	DECLARE_UNSAFE_GETTER(Float64, Float64, float64)
	DECLARE_UNSAFE_GETTER(StringRef, String, string)
	#undef DECLARE_UNSAFE_GETTER

	bool isComplete() const override { return true; }

	bool hasHierarchy() const override { return hierarchical_attribute; }

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
		Float32 float32_null_value;
		Float64 float64_null_value;
		String string_null_value;
		std::unique_ptr<PODArray<UInt8>> uint8_array;
		std::unique_ptr<PODArray<UInt16>> uint16_array;
		std::unique_ptr<PODArray<UInt32>> uint32_array;
		std::unique_ptr<PODArray<UInt64>> uint64_array;
		std::unique_ptr<PODArray<Int8>> int8_array;
		std::unique_ptr<PODArray<Int16>> int16_array;
		std::unique_ptr<PODArray<Int32>> int32_array;
		std::unique_ptr<PODArray<Int64>> int64_array;
		std::unique_ptr<PODArray<Float32>> float32_array;
		std::unique_ptr<PODArray<Float64>> float64_array;
		std::unique_ptr<Arena> string_arena;
		std::unique_ptr<PODArray<StringRef>> string_array;
	};

	attribute_t createAttributeWithType(const attribute_type type, const std::string & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case attribute_type::uint8:
				attr.uint8_null_value = DB::parse<UInt8>(null_value);
				attr.uint8_array.reset(new PODArray<UInt8>);
				attr.uint8_array->resize_fill(initial_array_size, attr.uint8_null_value);
				break;
			case attribute_type::uint16:
				attr.uint16_null_value = DB::parse<UInt16>(null_value);
				attr.uint16_array.reset(new PODArray<UInt16>);
				attr.uint16_array->resize_fill(initial_array_size, attr.uint16_null_value);
				break;
			case attribute_type::uint32:
				attr.uint32_null_value = DB::parse<UInt32>(null_value);
				attr.uint32_array.reset(new PODArray<UInt32>);
				attr.uint32_array->resize_fill(initial_array_size, attr.uint32_null_value);
				break;
			case attribute_type::uint64:
				attr.uint64_null_value = DB::parse<UInt64>(null_value);
				attr.uint64_array.reset(new PODArray<UInt64>);
				attr.uint64_array->resize_fill(initial_array_size, attr.uint64_null_value);
				break;
			case attribute_type::int8:
				attr.int8_null_value = DB::parse<Int8>(null_value);
				attr.int8_array.reset(new PODArray<Int8>);
				attr.int8_array->resize_fill(initial_array_size, attr.int8_null_value);
				break;
			case attribute_type::int16:
				attr.int16_null_value = DB::parse<Int16>(null_value);
				attr.int16_array.reset(new PODArray<Int16>);
				attr.int16_array->resize_fill(initial_array_size, attr.int16_null_value);
				break;
			case attribute_type::int32:
				attr.int32_null_value = DB::parse<Int32>(null_value);
				attr.int32_array.reset(new PODArray<Int32>);
				attr.int32_array->resize_fill(initial_array_size, attr.int32_null_value);
				break;
			case attribute_type::int64:
				attr.int64_null_value = DB::parse<Int64>(null_value);
				attr.int64_array.reset(new PODArray<Int64>);
				attr.int64_array->resize_fill(initial_array_size, attr.int64_null_value);
				break;
			case attribute_type::float32:
				attr.float32_null_value = DB::parse<Float32>(null_value);
				attr.float32_array.reset(new PODArray<Float32>);
				attr.float32_array->resize_fill(initial_array_size, attr.float32_null_value);
				break;
			case attribute_type::float64:
				attr.float64_null_value = DB::parse<Float64>(null_value);
				attr.float64_array.reset(new PODArray<Float64>);
				attr.float64_array->resize_fill(initial_array_size, attr.float64_null_value);
				break;
			case attribute_type::string:
				attr.string_null_value = null_value;
				attr.string_arena.reset(new Arena);
				attr.string_array.reset(new PODArray<StringRef>);
				attr.string_array->resize_fill(initial_array_size, attr.string_null_value);
				break;
		}

		return attr;
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
			case attribute_type::uint8:
			{
				if (id >= attribute.uint8_array->size())
					attribute.uint8_array->resize_fill(id, attribute.uint8_null_value);
				(*attribute.uint8_array)[id] = value.get<UInt64>();
				break;
			}
			case attribute_type::uint16:
			{
				if (id >= attribute.uint16_array->size())
					attribute.uint16_array->resize_fill(id, attribute.uint16_null_value);
				(*attribute.uint16_array)[id] = value.get<UInt64>();
				break;
			}
			case attribute_type::uint32:
			{
				if (id >= attribute.uint32_array->size())
					attribute.uint32_array->resize_fill(id, attribute.uint32_null_value);
				(*attribute.uint32_array)[id] = value.get<UInt64>();
				break;
			}
			case attribute_type::uint64:
			{
				if (id >= attribute.uint64_array->size())
					attribute.uint64_array->resize_fill(id, attribute.uint64_null_value);
				(*attribute.uint64_array)[id] = value.get<UInt64>();
				break;
			}
			case attribute_type::int8:
			{
				if (id >= attribute.int8_array->size())
					attribute.int8_array->resize_fill(id, attribute.int8_null_value);
				(*attribute.int8_array)[id] = value.get<Int64>();
				break;
			}
			case attribute_type::int16:
			{
				if (id >= attribute.int16_array->size())
					attribute.int16_array->resize_fill(id, attribute.int16_null_value);
				(*attribute.int16_array)[id] = value.get<Int64>();
				break;
			}
			case attribute_type::int32:
			{
				if (id >= attribute.int32_array->size())
					attribute.int32_array->resize_fill(id, attribute.int32_null_value);
				(*attribute.int32_array)[id] = value.get<Int64>();
				break;
			}
			case attribute_type::int64:
			{
				if (id >= attribute.int64_array->size())
					attribute.int64_array->resize_fill(id, attribute.int64_null_value);
				(*attribute.int64_array)[id] = value.get<Int64>();
				break;
			}
			case attribute_type::float32:
			{
				if (id >= attribute.float32_array->size())
					attribute.float32_array->resize_fill(id, attribute.float32_null_value);
				(*attribute.float32_array)[id] = value.get<Float64>();
				break;
			}
			case attribute_type::float64:
			{
				if (id >= attribute.float64_array->size())
					attribute.float64_array->resize_fill(id, attribute.float64_null_value);
				(*attribute.float64_array)[id] = value.get<Float64>();
				break;
			}
			case attribute_type::string:
			{
				if (id >= attribute.string_array->size())
					attribute.string_array->resize_fill(id, attribute.string_null_value);
				const auto & string = value.get<String>();
				const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
				(*attribute.string_array)[id] = StringRef{string_in_arena, string.size()};
				break;
			}
		};
	}

	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;
	const attribute_t * hierarchical_attribute = nullptr;

	DictionarySourcePtr source_ptr;
};

}
