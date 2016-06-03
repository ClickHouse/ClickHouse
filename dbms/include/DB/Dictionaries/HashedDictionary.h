#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Columns/ColumnString.h>
#include <ext/range.hpp>
#include <atomic>
#include <memory>
#include <tuple>


namespace DB
{

namespace ErrorCodes
{
	extern const int TYPE_MISMATCH;
	extern const int ARGUMENT_OUT_OF_BOUND;
	extern const int BAD_ARGUMENTS;
	extern const int DICTIONARY_IS_EMPTY;
}


class HashedDictionary final : public IDictionary
{
public:
	HashedDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime, bool require_nonempty)
		: name{name}, dict_struct(dict_struct), source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
		  require_nonempty(require_nonempty)
	{
		createAttributes();

		try
		{
			loadData();
			calculateBytesAllocated();
		}
		catch (...)
		{
			creation_exception = std::current_exception();
		}

		creation_time = std::chrono::system_clock::now();
	}

	HashedDictionary(const HashedDictionary & other)
		: HashedDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
	{}

	std::exception_ptr getCreationException() const override { return creation_exception; }

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "Hashed"; }

	std::size_t getBytesAllocated() const override { return bytes_allocated; }

	std::size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

	double getHitRate() const override { return 1.0; }

	std::size_t getElementCount() const override { return element_count; }

	double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

	bool isCached() const override { return false; }

	DictionaryPtr clone() const override { return std::make_unique<HashedDictionary>(*this); }

	const IDictionarySource * getSource() const override { return source_ptr.get(); }

	const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

	const DictionaryStructure & getStructure() const override { return dict_struct; }

	std::chrono::time_point<std::chrono::system_clock> getCreationTime() const override
	{
		return creation_time;
	}

	bool isInjective(const std::string & attribute_name) const override
	{
		return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
	}

	bool hasHierarchy() const override { return hierarchical_attribute; }

	void toParent(const PaddedPODArray<id_t> & ids, PaddedPODArray<id_t> & out) const override
	{
		const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

		getItems<UInt64>(*hierarchical_attribute, ids,
			[&] (const std::size_t row, const UInt64 value) { out[row] = value; },
			[&] (const std::size_t) { return null_value; });
	}

#define DECLARE(TYPE)\
	void get##TYPE(const std::string & attribute_name, const PaddedPODArray<id_t> & ids, PaddedPODArray<TYPE> & out) const\
	{\
		const auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		const auto null_value = std::get<TYPE>(attribute.null_values);\
		\
		getItems<TYPE>(attribute, ids,\
			[&] (const std::size_t row, const auto value) { out[row] = value; },\
			[&] (const std::size_t) { return null_value; });\
	}
	DECLARE(UInt8)
	DECLARE(UInt16)
	DECLARE(UInt32)
	DECLARE(UInt64)
	DECLARE(Int8)
	DECLARE(Int16)
	DECLARE(Int32)
	DECLARE(Int64)
	DECLARE(Float32)
	DECLARE(Float64)
#undef DECLARE
	void getString(const std::string & attribute_name, const PaddedPODArray<id_t> & ids, ColumnString * out) const
	{
		const auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		const auto & null_value = StringRef{std::get<String>(attribute.null_values)};

		getItems<StringRef>(attribute, ids,
			[&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
			[&] (const std::size_t) { return null_value; });
	}

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const PaddedPODArray<TYPE> & def,\
		PaddedPODArray<TYPE> & out) const\
	{\
		const auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, ids,\
			[&] (const std::size_t row, const auto value) { out[row] = value; },\
			[&] (const std::size_t row) { return def[row]; });\
	}
	DECLARE(UInt8)
	DECLARE(UInt16)
	DECLARE(UInt32)
	DECLARE(UInt64)
	DECLARE(Int8)
	DECLARE(Int16)
	DECLARE(Int32)
	DECLARE(Int64)
	DECLARE(Float32)
	DECLARE(Float64)
#undef DECLARE
	void getString(
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const ColumnString * const def,
		ColumnString * const out) const
	{
		const auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems<StringRef>(attribute, ids,
			[&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
			[&] (const std::size_t row) { return def->getDataAt(row); });
	}

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const TYPE & def, PaddedPODArray<TYPE> & out) const\
	{\
		const auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, ids,\
			[&] (const std::size_t row, const auto value) { out[row] = value; },\
			[&] (const std::size_t) { return def; });\
	}
	DECLARE(UInt8)
	DECLARE(UInt16)
	DECLARE(UInt32)
	DECLARE(UInt64)
	DECLARE(Int8)
	DECLARE(Int16)
	DECLARE(Int32)
	DECLARE(Int64)
	DECLARE(Float32)
	DECLARE(Float64)
#undef DECLARE
	void getString(
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const String & def,
		ColumnString * const out) const
	{
		const auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems<StringRef>(attribute, ids,
			[&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
			[&] (const std::size_t) { return StringRef{def}; });
	}

	void has(const PaddedPODArray<id_t> & ids, PaddedPODArray<UInt8> & out) const override
	{
		const auto & attribute = attributes.front();

		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: has<UInt8>(attribute, ids, out); break;
			case AttributeUnderlyingType::UInt16: has<UInt16>(attribute, ids, out); break;
			case AttributeUnderlyingType::UInt32: has<UInt32>(attribute, ids, out); break;
			case AttributeUnderlyingType::UInt64: has<UInt64>(attribute, ids, out); break;
			case AttributeUnderlyingType::Int8: has<Int8>(attribute, ids, out); break;
			case AttributeUnderlyingType::Int16: has<Int16>(attribute, ids, out); break;
			case AttributeUnderlyingType::Int32: has<Int32>(attribute, ids, out); break;
			case AttributeUnderlyingType::Int64: has<Int64>(attribute, ids, out); break;
			case AttributeUnderlyingType::Float32: has<Float32>(attribute, ids, out); break;
			case AttributeUnderlyingType::Float64: has<Float64>(attribute, ids, out); break;
			case AttributeUnderlyingType::String: has<StringRef>(attribute, ids, out); break;
		}
	}

private:
	template <typename Value> using CollectionType = HashMap<UInt64, Value>;
	template <typename Value> using CollectionPtrType = std::unique_ptr<CollectionType<Value>>;

	struct attribute_t final
	{
		AttributeUnderlyingType type;
		std::tuple<
			UInt8, UInt16, UInt32, UInt64,
			Int8, Int16, Int32, Int64,
			Float32, Float64,
			String> null_values;
		std::tuple<
			CollectionPtrType<UInt8>, CollectionPtrType<UInt16>, CollectionPtrType<UInt32>, CollectionPtrType<UInt64>,
			CollectionPtrType<Int8>, CollectionPtrType<Int16>, CollectionPtrType<Int32>, CollectionPtrType<Int64>,
			CollectionPtrType<Float32>, CollectionPtrType<Float64>,
			CollectionPtrType<StringRef>> maps;
		std::unique_ptr<Arena> string_arena;
	};

	void createAttributes()
	{
		const auto size = dict_struct.attributes.size();
		attributes.reserve(size);

		for (const auto & attribute : dict_struct.attributes)
		{
			attribute_index_by_name.emplace(attribute.name, attributes.size());
			attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

			if (attribute.hierarchical)
			{
				hierarchical_attribute = &attributes.back();

				if (hierarchical_attribute->type != AttributeUnderlyingType::UInt64)
					throw Exception{
						name + ": hierarchical attribute must be UInt64.",
						ErrorCodes::TYPE_MISMATCH
					};
			}
		}
	}

	void loadData()
	{
		auto stream = source_ptr->loadAll();
		stream->readPrefix();

		while (const auto block = stream->read())
		{
			const auto & id_column = *block.getByPosition(0).column;

			element_count += id_column.size();

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *block.getByPosition(attribute_idx + 1).column;
				auto & attribute = attributes[attribute_idx];

				for (const auto row_idx : ext::range(0, id_column.size()))
					setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]);
			}
		}

		stream->readSuffix();

		if (require_nonempty && 0 == element_count)
			throw Exception{
				name + ": dictionary source is empty and 'require_nonempty' property is set.",
				ErrorCodes::DICTIONARY_IS_EMPTY
			};
	}

	template <typename T>
	void addAttributeSize(const attribute_t & attribute)
	{
		const auto & map_ref = std::get<CollectionPtrType<T>>(attribute.maps);
		bytes_allocated += sizeof(CollectionType<T>) + map_ref->getBufferSizeInBytes();
		bucket_count = map_ref->getBufferSizeInCells();
	}

	void calculateBytesAllocated()
	{
		bytes_allocated += attributes.size() * sizeof(attributes.front());

		for (const auto & attribute : attributes)
		{
			switch (attribute.type)
			{
				case AttributeUnderlyingType::UInt8: addAttributeSize<UInt8>(attribute); break;
				case AttributeUnderlyingType::UInt16: addAttributeSize<UInt16>(attribute); break;
				case AttributeUnderlyingType::UInt32: addAttributeSize<UInt32>(attribute); break;
				case AttributeUnderlyingType::UInt64: addAttributeSize<UInt64>(attribute); break;
				case AttributeUnderlyingType::Int8: addAttributeSize<Int8>(attribute); break;
				case AttributeUnderlyingType::Int16: addAttributeSize<Int16>(attribute); break;
				case AttributeUnderlyingType::Int32: addAttributeSize<Int32>(attribute); break;
				case AttributeUnderlyingType::Int64: addAttributeSize<Int64>(attribute); break;
				case AttributeUnderlyingType::Float32: addAttributeSize<Float32>(attribute); break;
				case AttributeUnderlyingType::Float64: addAttributeSize<Float64>(attribute); break;
				case AttributeUnderlyingType::String:
				{
					addAttributeSize<StringRef>(attribute);
					bytes_allocated += sizeof(Arena) + attribute.string_arena->size();

					break;
				}
			}
		}
	}

	template <typename T>
	void createAttributeImpl(attribute_t & attribute, const Field & null_value)
	{
		std::get<T>(attribute.null_values) = null_value.get<typename NearestFieldType<T>::Type>();
		std::get<CollectionPtrType<T>>(attribute.maps) = std::make_unique<CollectionType<T>>();
	}

	attribute_t createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case AttributeUnderlyingType::UInt8: createAttributeImpl<UInt8>(attr, null_value); break;
			case AttributeUnderlyingType::UInt16: createAttributeImpl<UInt16>(attr, null_value); break;
			case AttributeUnderlyingType::UInt32: createAttributeImpl<UInt32>(attr, null_value); break;
			case AttributeUnderlyingType::UInt64: createAttributeImpl<UInt64>(attr, null_value); break;
			case AttributeUnderlyingType::Int8: createAttributeImpl<Int8>(attr, null_value); break;
			case AttributeUnderlyingType::Int16: createAttributeImpl<Int16>(attr, null_value); break;
			case AttributeUnderlyingType::Int32: createAttributeImpl<Int32>(attr, null_value); break;
			case AttributeUnderlyingType::Int64: createAttributeImpl<Int64>(attr, null_value); break;
			case AttributeUnderlyingType::Float32: createAttributeImpl<Float32>(attr, null_value); break;
			case AttributeUnderlyingType::Float64: createAttributeImpl<Float64>(attr, null_value); break;
			case AttributeUnderlyingType::String:
			{
				std::get<String>(attr.null_values) = null_value.get<String>();
				std::get<CollectionPtrType<StringRef>>(attr.maps) = std::make_unique<CollectionType<StringRef>>();
				attr.string_arena = std::make_unique<Arena>();
				break;
			}
		}

		return attr;
	}

	template <typename T, typename ValueSetter, typename DefaultGetter>
	void getItems(
		const attribute_t & attribute, const PaddedPODArray<id_t> & ids, ValueSetter && set_value,
		DefaultGetter && get_default) const
	{
		const auto & attr = *std::get<CollectionPtrType<T>>(attribute.maps);
		const auto rows = ext::size(ids);

		for (const auto i : ext::range(0, rows))
		{
			const auto it = attr.find(ids[i]);
			set_value(i, it != attr.end() ? it->second : get_default(i));
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
	}

	template <typename T>
	void setAttributeValueImpl(attribute_t & attribute, const id_t id, const T value)
	{
		auto & map = *std::get<CollectionPtrType<T>>(attribute.maps);
		map.insert({ id, value });
	}

	void setAttributeValue(attribute_t & attribute, const id_t id, const Field & value)
	{
		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: setAttributeValueImpl<UInt8>(attribute, id, value.get<UInt64>()); break;
			case AttributeUnderlyingType::UInt16: setAttributeValueImpl<UInt16>(attribute, id, value.get<UInt64>()); break;
			case AttributeUnderlyingType::UInt32: setAttributeValueImpl<UInt32>(attribute, id, value.get<UInt64>()); break;
			case AttributeUnderlyingType::UInt64: setAttributeValueImpl<UInt64>(attribute, id, value.get<UInt64>()); break;
			case AttributeUnderlyingType::Int8: setAttributeValueImpl<Int8>(attribute, id, value.get<Int64>()); break;
			case AttributeUnderlyingType::Int16: setAttributeValueImpl<Int16>(attribute, id, value.get<Int64>()); break;
			case AttributeUnderlyingType::Int32: setAttributeValueImpl<Int32>(attribute, id, value.get<Int64>()); break;
			case AttributeUnderlyingType::Int64: setAttributeValueImpl<Int64>(attribute, id, value.get<Int64>()); break;
			case AttributeUnderlyingType::Float32: setAttributeValueImpl<Float32>(attribute, id, value.get<Float64>()); break;
			case AttributeUnderlyingType::Float64: setAttributeValueImpl<Float64>(attribute, id, value.get<Float64>()); break;
			case AttributeUnderlyingType::String:
			{
				auto & map = *std::get<CollectionPtrType<StringRef>>(attribute.maps);
				const auto & string = value.get<String>();
				const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
				map.insert({ id, StringRef{string_in_arena, string.size()} });
				break;
			}
		}
	}

	const attribute_t & getAttribute(const std::string & attribute_name) const
	{
		const auto it = attribute_index_by_name.find(attribute_name);
		if (it == std::end(attribute_index_by_name))
			throw Exception{
				name + ": no such attribute '" + attribute_name + "'",
				ErrorCodes::BAD_ARGUMENTS
			};

		return attributes[it->second];
	}

	template <typename T>
	void has(const attribute_t & attribute, const PaddedPODArray<id_t> & ids, PaddedPODArray<UInt8> & out) const
	{
		const auto & attr = *std::get<CollectionPtrType<T>>(attribute.maps);
		const auto rows = ext::size(ids);

		for (const auto i : ext::range(0, rows))
			out[i] = attr.find(ids[i]) != std::end(attr);

		query_count.fetch_add(rows, std::memory_order_relaxed);
	}

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;
	const bool require_nonempty;

	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;
	const attribute_t * hierarchical_attribute = nullptr;

	std::size_t bytes_allocated = 0;
	std::size_t element_count = 0;
	std::size_t bucket_count = 0;
	mutable std::atomic<std::size_t> query_count{0};

	std::chrono::time_point<std::chrono::system_clock> creation_time;

	std::exception_ptr creation_exception;
};

}
