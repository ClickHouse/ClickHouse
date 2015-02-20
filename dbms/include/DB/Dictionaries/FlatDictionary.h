#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Columns/ColumnString.h>
#include <statdaemons/ext/range.hpp>
#include <vector>

namespace DB
{

const auto initial_array_size = 1024;
const auto max_array_size = 500000;

class FlatDictionary final : public IDictionary
{
public:
    FlatDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime)
		: name{name}, dict_struct(dict_struct),
		  source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime)
	{
		createAttributes();
		loadData();
	}

	FlatDictionary(const FlatDictionary & other)
		: FlatDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime}
	{}

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "FlatDictionary"; }

	bool isCached() const override { return false; }

	DictionaryPtr clone() const override { return std::make_unique<FlatDictionary>(*this); }

	const IDictionarySource * getSource() const override { return source_ptr.get(); }

	const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

	bool hasHierarchy() const override { return hierarchical_attribute; }

	id_t toParent(const id_t id) const override
	{
		const auto attr = hierarchical_attribute;

		switch (hierarchical_attribute->type)
		{
			case AttributeType::uint8: return id < attr->uint8_array->size() ? (*attr->uint8_array)[id] : attr->uint8_null_value;
			case AttributeType::uint16: return id < attr->uint16_array->size() ? (*attr->uint16_array)[id] : attr->uint16_null_value;
			case AttributeType::uint32: return id < attr->uint32_array->size() ? (*attr->uint32_array)[id] : attr->uint32_null_value;
			case AttributeType::uint64: return id < attr->uint64_array->size() ? (*attr->uint64_array)[id] : attr->uint64_null_value;
			case AttributeType::int8: return id < attr->int8_array->size() ? (*attr->int8_array)[id] : attr->int8_null_value;
			case AttributeType::int16: return id < attr->int16_array->size() ? (*attr->int16_array)[id] : attr->int16_null_value;
			case AttributeType::int32: return id < attr->int32_array->size() ? (*attr->int32_array)[id] : attr->int32_null_value;
			case AttributeType::int64: return id < attr->int64_array->size() ? (*attr->int64_array)[id] : attr->int64_null_value;
			case AttributeType::float32:
			case AttributeType::float64:
			case AttributeType::string:
				break;
		}

		throw Exception{
			"Hierarchical attribute has non-integer type " + toString(hierarchical_attribute->type),
			ErrorCodes::TYPE_MISMATCH
		};
	}

#define DECLARE_INDIVIDUAL_GETTER(TYPE, NAME, LC_TYPE) \
	TYPE get##NAME(const std::string & attribute_name, const id_t id) const override\
	{\
		const auto idx = getAttributeIndex(attribute_name);\
		const auto & attribute = attributes[idx];\
		if (attribute.type != AttributeType::LC_TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		if (id < attribute.LC_TYPE##_array->size())\
			return (*attribute.LC_TYPE##_array)[id];\
		return attribute.LC_TYPE##_null_value;\
	}
	DECLARE_INDIVIDUAL_GETTER(UInt8, UInt8, uint8)
	DECLARE_INDIVIDUAL_GETTER(UInt16, UInt16, uint16)
	DECLARE_INDIVIDUAL_GETTER(UInt32, UInt32, uint32)
	DECLARE_INDIVIDUAL_GETTER(UInt64, UInt64, uint64)
	DECLARE_INDIVIDUAL_GETTER(Int8, Int8, int8)
	DECLARE_INDIVIDUAL_GETTER(Int16, Int16, int16)
	DECLARE_INDIVIDUAL_GETTER(Int32, Int32, int32)
	DECLARE_INDIVIDUAL_GETTER(Int64, Int64, int64)
	DECLARE_INDIVIDUAL_GETTER(Float32, Float32, float32)
	DECLARE_INDIVIDUAL_GETTER(Float64, Float64, float64)
	DECLARE_INDIVIDUAL_GETTER(StringRef, String, string)
#undef DECLARE_INDIVIDUAL_GETTER

#define DECLARE_MULTIPLE_GETTER(TYPE, LC_TYPE)\
	void get##TYPE(const std::string & attribute_name, const PODArray<id_t> & ids, PODArray<TYPE> & out) const override\
	{\
		const auto idx = getAttributeIndex(attribute_name);\
		const auto & attribute = attributes[idx];\
		if (attribute.type != AttributeType::LC_TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		const auto & attr = *attribute.LC_TYPE##_array;\
		const auto null_value = attribute.LC_TYPE##_null_value;\
		\
		for (const auto i : ext::range(0, ids.size()))\
		{\
			const auto id = ids[i];\
			out[i] = id < attr.size() ? attr[id] : null_value;\
		}\
	}
	DECLARE_MULTIPLE_GETTER(UInt8, uint8)
	DECLARE_MULTIPLE_GETTER(UInt16, uint16)
	DECLARE_MULTIPLE_GETTER(UInt32, uint32)
	DECLARE_MULTIPLE_GETTER(UInt64, uint64)
	DECLARE_MULTIPLE_GETTER(Int8, int8)
	DECLARE_MULTIPLE_GETTER(Int16, int16)
	DECLARE_MULTIPLE_GETTER(Int32, int32)
	DECLARE_MULTIPLE_GETTER(Int64, int64)
	DECLARE_MULTIPLE_GETTER(Float32, float32)
	DECLARE_MULTIPLE_GETTER(Float64, float64)
#undef DECLARE_MULTIPLE_GETTER
	void getString(const std::string & attribute_name, const PODArray<id_t> & ids, ColumnString * out) const override
	{
		const auto idx = getAttributeIndex(attribute_name);
		const auto & attribute = attributes[idx];
		if (attribute.type != AttributeType::string)
			throw Exception{
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		const auto & attr = *attribute.string_array;
		const auto null_value = attribute.string_null_value;

		for (const auto i : ext::range(0, ids.size()))
		{
			const auto id = ids[i];
			const auto string_ref = id < attr.size() ? attr[id] : null_value;
			out->insertData(string_ref.data, string_ref.size);
		}
	}

private:
	struct attribute_t
	{
		AttributeType type;
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

	void createAttributes()
	{
		const auto size = dict_struct.attributes.size();
		attributes.reserve(size);
		for (const auto & attribute : dict_struct.attributes)
		{
			attribute_index_by_name.emplace(attribute.name, attributes.size());
			attributes.push_back(std::move(createAttributeWithType(getAttributeTypeByName(attribute.type),
				attribute.null_value)));

			if (attribute.hierarchical)
				hierarchical_attribute = &attributes.back();
		}
	}

	void loadData()
	{
		auto stream = source_ptr->loadAll();
		stream->readPrefix();

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

		stream->readSuffix();
	}

	attribute_t createAttributeWithType(const AttributeType type, const std::string & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case AttributeType::uint8:
				attr.uint8_null_value = DB::parse<UInt8>(null_value);
				attr.uint8_array.reset(new PODArray<UInt8>);
				attr.uint8_array->resize_fill(initial_array_size, attr.uint8_null_value);
				break;
			case AttributeType::uint16:
				attr.uint16_null_value = DB::parse<UInt16>(null_value);
				attr.uint16_array.reset(new PODArray<UInt16>);
				attr.uint16_array->resize_fill(initial_array_size, attr.uint16_null_value);
				break;
			case AttributeType::uint32:
				attr.uint32_null_value = DB::parse<UInt32>(null_value);
				attr.uint32_array.reset(new PODArray<UInt32>);
				attr.uint32_array->resize_fill(initial_array_size, attr.uint32_null_value);
				break;
			case AttributeType::uint64:
				attr.uint64_null_value = DB::parse<UInt64>(null_value);
				attr.uint64_array.reset(new PODArray<UInt64>);
				attr.uint64_array->resize_fill(initial_array_size, attr.uint64_null_value);
				break;
			case AttributeType::int8:
				attr.int8_null_value = DB::parse<Int8>(null_value);
				attr.int8_array.reset(new PODArray<Int8>);
				attr.int8_array->resize_fill(initial_array_size, attr.int8_null_value);
				break;
			case AttributeType::int16:
				attr.int16_null_value = DB::parse<Int16>(null_value);
				attr.int16_array.reset(new PODArray<Int16>);
				attr.int16_array->resize_fill(initial_array_size, attr.int16_null_value);
				break;
			case AttributeType::int32:
				attr.int32_null_value = DB::parse<Int32>(null_value);
				attr.int32_array.reset(new PODArray<Int32>);
				attr.int32_array->resize_fill(initial_array_size, attr.int32_null_value);
				break;
			case AttributeType::int64:
				attr.int64_null_value = DB::parse<Int64>(null_value);
				attr.int64_array.reset(new PODArray<Int64>);
				attr.int64_array->resize_fill(initial_array_size, attr.int64_null_value);
				break;
			case AttributeType::float32:
				attr.float32_null_value = DB::parse<Float32>(null_value);
				attr.float32_array.reset(new PODArray<Float32>);
				attr.float32_array->resize_fill(initial_array_size, attr.float32_null_value);
				break;
			case AttributeType::float64:
				attr.float64_null_value = DB::parse<Float64>(null_value);
				attr.float64_array.reset(new PODArray<Float64>);
				attr.float64_array->resize_fill(initial_array_size, attr.float64_null_value);
				break;
			case AttributeType::string:
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
			case AttributeType::uint8:
			{
				if (id >= attribute.uint8_array->size())
					attribute.uint8_array->resize_fill(id, attribute.uint8_null_value);
				(*attribute.uint8_array)[id] = value.get<UInt64>();
				break;
			}
			case AttributeType::uint16:
			{
				if (id >= attribute.uint16_array->size())
					attribute.uint16_array->resize_fill(id, attribute.uint16_null_value);
				(*attribute.uint16_array)[id] = value.get<UInt64>();
				break;
			}
			case AttributeType::uint32:
			{
				if (id >= attribute.uint32_array->size())
					attribute.uint32_array->resize_fill(id, attribute.uint32_null_value);
				(*attribute.uint32_array)[id] = value.get<UInt64>();
				break;
			}
			case AttributeType::uint64:
			{
				if (id >= attribute.uint64_array->size())
					attribute.uint64_array->resize_fill(id, attribute.uint64_null_value);
				(*attribute.uint64_array)[id] = value.get<UInt64>();
				break;
			}
			case AttributeType::int8:
			{
				if (id >= attribute.int8_array->size())
					attribute.int8_array->resize_fill(id, attribute.int8_null_value);
				(*attribute.int8_array)[id] = value.get<Int64>();
				break;
			}
			case AttributeType::int16:
			{
				if (id >= attribute.int16_array->size())
					attribute.int16_array->resize_fill(id, attribute.int16_null_value);
				(*attribute.int16_array)[id] = value.get<Int64>();
				break;
			}
			case AttributeType::int32:
			{
				if (id >= attribute.int32_array->size())
					attribute.int32_array->resize_fill(id, attribute.int32_null_value);
				(*attribute.int32_array)[id] = value.get<Int64>();
				break;
			}
			case AttributeType::int64:
			{
				if (id >= attribute.int64_array->size())
					attribute.int64_array->resize_fill(id, attribute.int64_null_value);
				(*attribute.int64_array)[id] = value.get<Int64>();
				break;
			}
			case AttributeType::float32:
			{
				if (id >= attribute.float32_array->size())
					attribute.float32_array->resize_fill(id, attribute.float32_null_value);
				(*attribute.float32_array)[id] = value.get<Float64>();
				break;
			}
			case AttributeType::float64:
			{
				if (id >= attribute.float64_array->size())
					attribute.float64_array->resize_fill(id, attribute.float64_null_value);
				(*attribute.float64_array)[id] = value.get<Float64>();
				break;
			}
			case AttributeType::string:
			{
				if (id >= attribute.string_array->size())
					attribute.string_array->resize_fill(id, attribute.string_null_value);
				const auto & string = value.get<String>();
				const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
				(*attribute.string_array)[id] = StringRef{string_in_arena, string.size()};
				break;
			}
		}
	}

	std::size_t getAttributeIndex(const std::string & attribute_name) const
	{
		const auto it = attribute_index_by_name.find(attribute_name);
		if (it == std::end(attribute_index_by_name))
			throw Exception{
				"No such attribute '" + attribute_name + "'",
				ErrorCodes::BAD_ARGUMENTS
			};

		return it->second;
	}

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;

	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;
	const attribute_t * hierarchical_attribute = nullptr;
};

}
