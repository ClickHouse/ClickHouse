#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Columns/ColumnString.h>
#include <statdaemons/ext/scope_guard.hpp>
#include <cmath>
#include <chrono>
#include <vector>
#include <map>

namespace DB
{

constexpr std::chrono::milliseconds spinlock_wait_time{10};

class CacheDictionary final : public IDictionary
{
public:
	CacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
		const std::size_t size)
		: name{name}, dict_struct(dict_struct),
		  source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
		  size{round_up_to_power_of_two(size)},
		  cells(this->size, cell{dict_struct.attributes.size()})
	{
		if (!this->source_ptr->supportsSelectiveLoad())
			throw Exception{
				"Source cannot be used with CacheDictionary",
				ErrorCodes::UNSUPPORTED_METHOD
			};

		createAttributes();
	}

	CacheDictionary(const CacheDictionary & other)
		: CacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
	{}

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "CacheDictionary"; }

	bool isCached() const override { return true; }

	DictionaryPtr clone() const override { return std::make_unique<CacheDictionary>(*this); }

	const IDictionarySource * getSource() const override { return source_ptr.get(); }

	const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

	bool hasHierarchy() const override { return false; }

	id_t toParent(const id_t id) const override { return 0; }

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
		\
        return getItem<TYPE>(getAttributeIndex(attribute_name), id);\
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
	void get##TYPE(const std::string & attribute_name, const PODArray<UInt64> & ids, PODArray<TYPE> & out) const override\
	{\
		const auto idx = getAttributeIndex(attribute_name);\
		const auto & attribute = attributes[idx];\
		if (attribute.type != AttributeType::LC_TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		for (const auto i : ext::range(0, ids.size()))\
			out[i] = getItem<TYPE>(idx, ids[i]);\
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
	void getString(const std::string & attribute_name, const PODArray<UInt64> & ids, ColumnString * out) const override
	{
		const auto idx = getAttributeIndex(attribute_name);
		const auto & attribute = attributes[idx];
		if (attribute.type != AttributeType::string)
			throw Exception{
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		for (const auto i : ext::range(0, ids.size()))
		{
			const auto string_ref = getItem<StringRef>(idx, ids[i]);
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

	attribute_t createAttributeWithType(const AttributeType type, const std::string & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case AttributeType::uint8:
				attr.uint8_null_value = DB::parse<UInt8>(null_value);
				break;
			case AttributeType::uint16:
				attr.uint16_null_value = DB::parse<UInt16>(null_value);
				break;
			case AttributeType::uint32:
				attr.uint32_null_value = DB::parse<UInt32>(null_value);
				break;
			case AttributeType::uint64:
				attr.uint64_null_value = DB::parse<UInt64>(null_value);
				break;
			case AttributeType::int8:
				attr.int8_null_value = DB::parse<Int8>(null_value);
				break;
			case AttributeType::int16:
				attr.int16_null_value = DB::parse<Int16>(null_value);
				break;
			case AttributeType::int32:
				attr.int32_null_value = DB::parse<Int32>(null_value);
				break;
			case AttributeType::int64:
				attr.int64_null_value = DB::parse<Int64>(null_value);
				break;
			case AttributeType::float32:
				attr.float32_null_value = DB::parse<Float32>(null_value);
				break;
			case AttributeType::float64:
				attr.float64_null_value = DB::parse<Float64>(null_value);
				break;
			case AttributeType::string:
				attr.string_null_value = null_value;
				break;
		}

		return attr;
	}

	union item
	{
		UInt8 uint8_value;
		UInt16 uint16_value;
		UInt32 uint32_value;
		UInt64 uint64_value;
		Int8 int8_value;
		Int16 int16_value;
		Int32 int32_value;
		Int64 int64_value;
		Float32 float32_value;
		Float64 float64_value;
		StringRef string_value;

		item() : string_value{} {}

		template <typename T> inline T get() const = delete;
	};

	struct cell
	{
		std::atomic_flag lock{false};
		id_t id{};
		std::vector<item> attrs;
		std::chrono::system_clock::time_point expires_at{};

		cell() = default;
		cell(const std::size_t attribute_count) : attrs(attribute_count) {}
		cell(const cell & other) { *this = other; }

		cell & operator=(const cell & other)
		{
			id = other.id;
			attrs = other.attrs;
			expires_at = other.expires_at;

			return *this;
		}

		bool hasExpired() const { return std::chrono::system_clock::now() >= expires_at; }
	};

	template <typename T>
	T getItem(const std::size_t attribute_idx, const id_t id) const
	{
		const auto hash = intHash64(id);
		const auto idx = hash % size;
		auto & cell = cells[idx];

		/// spinlock with a bit of throttling
		while (cell.lock.test_and_set(std::memory_order_acquire))
			std::this_thread::sleep_for(spinlock_wait_time);

		SCOPE_EXIT(
			cell.lock.clear(std::memory_order_release);
		);

		if (cell.id != id || cell.hasExpired())
			populateCellForId(cell, id);

		return cell.attrs[attribute_idx].get<T>();
	}

	void populateCellForId(cell & cell, const id_t id) const
	{
		auto stream = source_ptr->loadId(id);
		stream->readPrefix();

		auto empty_response = true;

		while (const auto block = stream->read())
		{
			if (!empty_response)
				throw Exception{
					"Stream returned from loadId contains more than one block",
					ErrorCodes::LOGICAL_ERROR
				};

			if (block.rowsInFirstColumn() != 1)
				throw Exception{
					"Block has more than one row",
					ErrorCodes::LOGICAL_ERROR
				};

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *block.getByPosition(attribute_idx + 1).column;
				auto & attribute = attributes[attribute_idx];

				setAttributeValue(cell.attrs[attribute_idx], attribute, attribute_column[0]);
			}

			empty_response = false;
		}

		stream->readSuffix();

		if (empty_response)
			setCellDefaults(cell);

		cell.id = id;
		cell.expires_at = std::chrono::system_clock::now() + std::chrono::seconds{dict_lifetime.min_sec};
	}

	void setAttributeValue(item & item, const attribute_t & attribute, const Field & value) const
	{
		switch (attribute.type)
		{
			case AttributeType::uint8: item.uint8_value = value.get<UInt64>(); break;
			case AttributeType::uint16: item.uint16_value = value.get<UInt64>(); break;
			case AttributeType::uint32: item.uint32_value = value.get<UInt64>(); break;
			case AttributeType::uint64: item.uint64_value = value.get<UInt64>(); break;
			case AttributeType::int8: item.int8_value = value.get<Int64>(); break;
			case AttributeType::int16: item.int16_value = value.get<Int64>(); break;
			case AttributeType::int32: item.int32_value = value.get<Int64>(); break;
			case AttributeType::int64: item.int64_value = value.get<Int64>(); break;
			case AttributeType::float32: item.float32_value = value.get<Float64>(); break;
			case AttributeType::float64: item.float64_value = value.get<Float64>(); break;
			case AttributeType::string:
			{
				const auto & string = value.get<String>();
				auto & string_ref = item.string_value;
				if (string_ref.data && string_ref.data != attribute.string_null_value.data())
					delete[] string_ref.data;

				const auto size = string.size();
				if (size > 0)
				{
					const auto string_ptr = new char[size + 1];
					std::copy(string.data(), string.data() + size + 1, string_ptr);
					string_ref = StringRef{string_ptr, size};
				}
				else
					string_ref = {};

				break;
			}
		}
	}

	void setCellDefaults(cell & cell) const
	{
		for (const auto attribute_idx : ext::range(0, attributes.size()))
		{
			auto & attribute = attributes[attribute_idx];
			auto & item = cell.attrs[attribute_idx];

			switch (attribute.type)
			{
				case AttributeType::uint8: item.uint8_value = attribute.uint8_null_value; break;
				case AttributeType::uint16: item.uint16_value = attribute.uint16_null_value; break;
				case AttributeType::uint32: item.uint32_value = attribute.uint32_null_value; break;
				case AttributeType::uint64: item.uint64_value = attribute.uint64_null_value; break;
				case AttributeType::int8: item.int8_value = attribute.int8_null_value; break;
				case AttributeType::int16: item.int16_value = attribute.int16_null_value; break;
				case AttributeType::int32: item.int32_value = attribute.int32_null_value; break;
				case AttributeType::int64: item.int64_value = attribute.int64_null_value; break;
				case AttributeType::float32: item.float32_value = attribute.float32_null_value; break;
				case AttributeType::float64: item.float64_value = attribute.float64_null_value; break;
				case AttributeType::string:
				{
					auto & string_ref = item.string_value;
					if (string_ref.data && string_ref.data != attribute.string_null_value.data())
						delete[] string_ref.data;

					string_ref = attribute.string_null_value;

					break;
				}
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

	static std::size_t round_up_to_power_of_two(std::size_t n)
	{
		--n;
		n |= n >> 1;
		n |= n >> 2;
		n |= n >> 4;
		n |= n >> 8;
		n |= n >> 16;
		n |= n >> 32;
		++n;

		return n;
	}

	static std::uint64_t getSeed()
	{
		timespec ts;
		clock_gettime(CLOCK_MONOTONIC, &ts);
		return ts.tv_nsec ^ getpid();
	}

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;

	const std::size_t size;
	mutable std::vector<cell> cells;
	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;
	const attribute_t * hierarchical_attribute = nullptr;
};

template <> inline UInt8 CacheDictionary::item::get<UInt8>() const { return uint8_value; }
template <> inline UInt16 CacheDictionary::item::get<UInt16>() const { return uint16_value; }
template <> inline UInt32 CacheDictionary::item::get<UInt32>() const { return uint32_value; }
template <> inline UInt64 CacheDictionary::item::get<UInt64>() const { return uint64_value; }
template <> inline Int8 CacheDictionary::item::get<Int8>() const { return int8_value; }
template <> inline Int16 CacheDictionary::item::get<Int16>() const { return int16_value; }
template <> inline Int32 CacheDictionary::item::get<Int32>() const { return int32_value; }
template <> inline Int64 CacheDictionary::item::get<Int64>() const { return int64_value; }
template <> inline Float32 CacheDictionary::item::get<Float32>() const { return float32_value; }
template <> inline Float64 CacheDictionary::item::get<Float64>() const { return float64_value; }
template <> inline StringRef CacheDictionary::item::get<StringRef>() const { return string_value; }

}
