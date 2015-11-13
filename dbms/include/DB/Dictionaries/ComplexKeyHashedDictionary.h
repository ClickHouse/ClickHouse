#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Core/StringRef.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Columns/ColumnString.h>
#include <ext/range.hpp>
#include <atomic>
#include <memory>
#include <tuple>


namespace DB
{

class ComplexKeyHashedDictionary final : public IDictionaryBase
{
public:
	ComplexKeyHashedDictionary(
		const std::string & name, const DictionaryStructure & dict_struct, DictionarySourcePtr source_ptr,
		const DictionaryLifetime dict_lifetime, bool require_nonempty)
	: name{name}, dict_struct(dict_struct), source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
	  require_nonempty(require_nonempty), key_description{createKeyDescription(dict_struct)}
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

	ComplexKeyHashedDictionary(const ComplexKeyHashedDictionary & other)
		: ComplexKeyHashedDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
	{}

	std::string getKeyDescription() const { return key_description; };

	std::exception_ptr getCreationException() const override { return creation_exception; }

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "ComplexKeyHashed"; }

	std::size_t getBytesAllocated() const override { return bytes_allocated; }

	std::size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

	double getHitRate() const override { return 1.0; }

	std::size_t getElementCount() const override { return element_count; }

	double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

	bool isCached() const override { return false; }

	DictionaryPtr clone() const override { return std::make_unique<ComplexKeyHashedDictionary>(*this); }

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

#define DECLARE_MULTIPLE_GETTER(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		PODArray<TYPE> & out) const\
	{\
		validateKeyColumns(key_types);\
		\
		const auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, key_columns, out);\
	}
	DECLARE_MULTIPLE_GETTER(UInt8)
	DECLARE_MULTIPLE_GETTER(UInt16)
	DECLARE_MULTIPLE_GETTER(UInt32)
	DECLARE_MULTIPLE_GETTER(UInt64)
	DECLARE_MULTIPLE_GETTER(Int8)
	DECLARE_MULTIPLE_GETTER(Int16)
	DECLARE_MULTIPLE_GETTER(Int32)
	DECLARE_MULTIPLE_GETTER(Int64)
	DECLARE_MULTIPLE_GETTER(Float32)
	DECLARE_MULTIPLE_GETTER(Float64)
#undef DECLARE_MULTIPLE_GETTER
	void getString(
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
		ColumnString * out) const
	{
		validateKeyColumns(key_types);

		const auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		const auto & attr = *std::get<MapPointerType<StringRef>>(attribute.maps);
		const auto & null_value = StringRef{std::get<String>(attribute.null_values)};

		const auto keys_size = key_columns.size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;

		const auto rows = key_columns.front()->size();
		for (const auto i : ext::range(0, rows))
		{
			const auto key = placeKeysInPool(i, key_columns, keys, temporary_keys_pool);

			const auto it = attr.find(key);
			const auto string_ref = it != attr.end() ? it->second : null_value;
			out->insertData(string_ref.data, string_ref.size);

			temporary_keys_pool.rollback(key.size);
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
	}

#define DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		const PODArray<TYPE> & def, PODArray<TYPE> & out) const\
	{\
 		validateKeyColumns(key_types);\
 		\
		const auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, key_columns, def, out);\
	}
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(UInt8)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(UInt16)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(UInt32)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(UInt64)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(Int8)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(Int16)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(Int32)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(Int64)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(Float32)
	DECLARE_MULTIPLE_GETTER_WITH_DEFAULT(Float64)
#undef DECLARE_MULTIPLE_GETTER_WITH_DEFAULT
	void getString(
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
		const ColumnString * const def, ColumnString * const out) const
	{
 		validateKeyColumns(key_types);

		const auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		const auto & attr = *std::get<MapPointerType<StringRef>>(attribute.maps);

		const auto keys_size = key_columns.size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;

		const auto rows = key_columns.front()->size();
		for (const auto i : ext::range(0, rows))
		{
			const auto key = placeKeysInPool(i, key_columns, keys, temporary_keys_pool);

			const auto it = attr.find(key);
			const auto string_ref = it != attr.end() ? it->second : def->getDataAt(i);
			out->insertData(string_ref.data, string_ref.size);

			temporary_keys_pool.rollback(key.size);
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
	}

private:
	template <typename Value> using MapType = HashMapWithSavedHash<StringRef, Value, StringRefHash>;
	template <typename Value> using MapPointerType = std::unique_ptr<MapType<Value>>;

	struct attribute_t final
	{
		AttributeUnderlyingType type;
		std::tuple<
			UInt8, UInt16, UInt32, UInt64,
			Int8, Int16, Int32, Int64,
			Float32, Float64,
			String> null_values;
		std::tuple<
			MapPointerType<UInt8>, MapPointerType<UInt16>, MapPointerType<UInt32>, MapPointerType<UInt64>,
			MapPointerType<Int8>, MapPointerType<Int16>, MapPointerType<Int32>, MapPointerType<Int64>,
			MapPointerType<Float32>, MapPointerType<Float64>,
			MapPointerType<StringRef>> maps;
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
				throw Exception{
					name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
					ErrorCodes::TYPE_MISMATCH
				};
		}
	}

	void loadData()
	{
		auto stream = source_ptr->loadAll();
		stream->readPrefix();

		/// created upfront to avoid excess allocations
		const auto keys_size = dict_struct.key->size();
		StringRefs keys(keys_size);

		const auto attributes_size = attributes.size();

		while (const auto block = stream->read())
		{
			const auto rows = block.rowsInFirstColumn();
			element_count += rows;

			const auto key_column_ptrs = ext::map<ConstColumnPlainPtrs>(ext::range(0, keys_size),
				[&] (const std::size_t attribute_idx) {
					return block.getByPosition(attribute_idx).column.get();
				});

			const auto attribute_column_ptrs = ext::map<ConstColumnPlainPtrs>(ext::range(0, attributes_size),
				[&] (const std::size_t attribute_idx) {
					return block.getByPosition(keys_size + attribute_idx).column.get();
				});

			for (const auto row_idx : ext::range(0, rows))
			{
				/// calculate key once per row
				const auto key = placeKeysInPool(row_idx, key_column_ptrs, keys, keys_pool);

				auto should_rollback = false;

				for (const auto attribute_idx : ext::range(0, attributes_size))
				{
					const auto & attribute_column = *attribute_column_ptrs[attribute_idx];
					auto & attribute = attributes[attribute_idx];
					const auto inserted = setAttributeValue(attribute, key, attribute_column[row_idx]);
					if (!inserted)
						should_rollback = true;
				}

				/// @note on multiple equal keys the mapped value for the first one is stored
				if (should_rollback)
					keys_pool.rollback(key.size);
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
		const auto & map_ref = std::get<MapPointerType<T>>(attribute.maps);
		bytes_allocated += sizeof(MapType<T>) + map_ref->getBufferSizeInBytes();
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

		bytes_allocated += keys_pool.size();
	}

	template <typename T>
	void createAttributeImpl(attribute_t & attribute, const Field & null_value)
	{
		std::get<T>(attribute.null_values) = null_value.get<typename NearestFieldType<T>::Type>();
		std::get<MapPointerType<T>>(attribute.maps) = std::make_unique<MapType<T>>();
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
				std::get<MapPointerType<StringRef>>(attr.maps) = std::make_unique<MapType<StringRef>>();
				attr.string_arena = std::make_unique<Arena>();
				break;
			}
		}

		return attr;
	}

	static std::string createKeyDescription(const DictionaryStructure & dict_struct)
	{
		std::ostringstream out;

		out << '(';

		auto first = true;
		for (const auto & key : *dict_struct.key)
		{
			if (!first)
				out << ", ";

			first = false;

			out << key.type->getName();
		}

		out << ')';

		return out.str();
	}

	void validateKeyColumns(const DataTypes & key_types) const
	{
		if (key_types.size() != dict_struct.key->size())
			throw Exception{
				"Key structure does not match, expected " + key_description,
				ErrorCodes::TYPE_MISMATCH
			};

		for (const auto i : ext::range(0, key_types.size()))
		{
			const auto & expected_type = (*dict_struct.key)[i].type->getName();
			const auto & actual_type = key_types[i]->getName();

			if (expected_type != actual_type)
				throw Exception{
					"Key type at position " + std::to_string(i) + " does not match, expected " + expected_type +
						", found " + actual_type,
					ErrorCodes::TYPE_MISMATCH
				};
		}
	}

	template <typename T>
	void getItems(const attribute_t & attribute, const ConstColumnPlainPtrs & key_columns, PODArray<T> & out) const
	{
		const auto & attr = *std::get<MapPointerType<T>>(attribute.maps);
		const auto null_value = std::get<T>(attribute.null_values);

		const auto keys_size = key_columns.size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;

		const auto rows = key_columns.front()->size();
		for (const auto i : ext::range(0, rows))
		{
			/// copy key data to arena so it is contiguous and return StringRef to it
			const auto key = placeKeysInPool(i, key_columns, keys, temporary_keys_pool);

			const auto it = attr.find(key);
			out[i] = it != attr.end() ? it->second : null_value;

			/// free memory allocated for key
			temporary_keys_pool.rollback(key.size);
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
	}

	template <typename T>
	void getItems(
 		const attribute_t & attribute, const ConstColumnPlainPtrs & key_columns, const PODArray<T> & def,
 		PODArray<T> & out) const
	{
		const auto & attr = *std::get<MapPointerType<T>>(attribute.maps);

		const auto keys_size = key_columns.size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;

		const auto rows = key_columns.front()->size();
		for (const auto i : ext::range(0, rows))
		{
			/// copy key data to arena so it is contiguous and return StringRef to it
			const auto key = placeKeysInPool(i, key_columns, keys, temporary_keys_pool);

			const auto it = attr.find(key);
			out[i] = it != attr.end() ? it->second : def[i];

			/// free memory allocated for key
			temporary_keys_pool.rollback(key.size);
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
	}

	template <typename T>
	bool setAttributeValueImpl(attribute_t & attribute, const StringRef key, const T value)
	{
		auto & map = *std::get<MapPointerType<T>>(attribute.maps);
		const auto pair = map.insert({ key, value });
		return pair.second;
	}

	bool setAttributeValue(attribute_t & attribute, const StringRef key, const Field & value)
	{
		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: return setAttributeValueImpl<UInt8>(attribute, key, value.get<UInt64>());
			case AttributeUnderlyingType::UInt16: return setAttributeValueImpl<UInt16>(attribute, key, value.get<UInt64>());
			case AttributeUnderlyingType::UInt32: return setAttributeValueImpl<UInt32>(attribute, key, value.get<UInt64>());
			case AttributeUnderlyingType::UInt64: return setAttributeValueImpl<UInt64>(attribute, key, value.get<UInt64>());
			case AttributeUnderlyingType::Int8: return setAttributeValueImpl<Int8>(attribute, key, value.get<Int64>());
			case AttributeUnderlyingType::Int16: return setAttributeValueImpl<Int16>(attribute, key, value.get<Int64>());
			case AttributeUnderlyingType::Int32: return setAttributeValueImpl<Int32>(attribute, key, value.get<Int64>());
			case AttributeUnderlyingType::Int64: return setAttributeValueImpl<Int64>(attribute, key, value.get<Int64>());
			case AttributeUnderlyingType::Float32: return setAttributeValueImpl<Float32>(attribute, key, value.get<Float64>());
			case AttributeUnderlyingType::Float64: return setAttributeValueImpl<Float64>(attribute, key, value.get<Float64>());
			case AttributeUnderlyingType::String:
			{
				auto & map = *std::get<MapPointerType<StringRef>>(attribute.maps);
				const auto & string = value.get<String>();
				const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
				const auto pair = map.insert({ key, StringRef{string_in_arena, string.size()} });
				return pair.second;
			}
		}

		return {};
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

	static StringRef placeKeysInPool(
		const std::size_t row, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool)
	{
		const auto keys_size = key_columns.size();
		size_t sum_keys_size{};
		for (const auto i : ext::range(0, keys_size))
		{
			keys[i] = key_columns[i]->getDataAtWithTerminatingZero(row);
			sum_keys_size += keys[i].size;
		}

		const auto res = pool.alloc(sum_keys_size);
		auto place = res;

		for (size_t j = 0; j < keys_size; ++j)
		{
			memcpy(place, keys[j].data, keys[j].size);
			place += keys[j].size;
		}

		return { res, sum_keys_size };
	}

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;
	const bool require_nonempty;
	const std::string key_description;

	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;
	Arena keys_pool;

	std::size_t bytes_allocated = 0;
	std::size_t element_count = 0;
	std::size_t bucket_count = 0;
	mutable std::atomic<std::size_t> query_count{0};

	std::chrono::time_point<std::chrono::system_clock> creation_time;

	std::exception_ptr creation_exception;
};


}
