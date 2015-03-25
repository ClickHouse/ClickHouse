#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Common/HashTable/HashMap.h>
#include <statdaemons/ext/scope_guard.hpp>
#include <Poco/RWLock.h>
#include <cmath>
#include <atomic>
#include <chrono>
#include <vector>
#include <map>
#include <tuple>

namespace DB
{

class CacheDictionary final : public IDictionary
{
public:
	CacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
		const std::size_t size)
		: name{name}, dict_struct(dict_struct),
		  source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
		  size{round_up_to_power_of_two(size)},
		  cells{this->size}
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

	std::string getTypeName() const override { return "Cache"; }

	std::size_t getBytesAllocated() const override { return bytes_allocated; }

	double getHitRate() const override
	{
		return static_cast<double>(hit_count.load(std::memory_order_acquire)) /
			query_count.load(std::memory_order_relaxed);
	}

	std::size_t getElementCount() const override { return element_count.load(std::memory_order_relaxed); }

	double getLoadFactor() const override
	{
		return static_cast<double>(element_count.load(std::memory_order_relaxed)) / size;
	}

	bool isCached() const override { return true; }

	DictionaryPtr clone() const override { return std::make_unique<CacheDictionary>(*this); }

	const IDictionarySource * getSource() const override { return source_ptr.get(); }

	const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

	const DictionaryStructure & getStructure() const override { return dict_struct; }

	std::chrono::time_point<std::chrono::system_clock> getCreationTime() const override
	{
		return creation_time;
	}

	bool hasHierarchy() const override { return hierarchical_attribute; }

	id_t toParent(const id_t id) const override
	{
		PODArray<UInt64> ids{1, id};
		PODArray<UInt64> out{1};
		getItems<UInt64>(*hierarchical_attribute, ids, out);
		return out.front();
	}

	void toParent(const PODArray<id_t> & ids, PODArray<id_t> & out) const override
	{
		getItems<UInt64>(*hierarchical_attribute, ids, out);
	}

#define DECLARE_INDIVIDUAL_GETTER(TYPE) \
	TYPE get##TYPE(const std::string & attribute_name, const id_t id) const override\
    {\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		PODArray<UInt64> ids{1, id};\
		PODArray<TYPE> out{1};\
		getItems<TYPE>(attribute, ids, out);\
        return out.front();\
	}
	DECLARE_INDIVIDUAL_GETTER(UInt8)
	DECLARE_INDIVIDUAL_GETTER(UInt16)
	DECLARE_INDIVIDUAL_GETTER(UInt32)
	DECLARE_INDIVIDUAL_GETTER(UInt64)
	DECLARE_INDIVIDUAL_GETTER(Int8)
	DECLARE_INDIVIDUAL_GETTER(Int16)
	DECLARE_INDIVIDUAL_GETTER(Int32)
	DECLARE_INDIVIDUAL_GETTER(Int64)
	DECLARE_INDIVIDUAL_GETTER(Float32)
	DECLARE_INDIVIDUAL_GETTER(Float64)
#undef DECLARE_INDIVIDUAL_GETTER
	String getString(const std::string & attribute_name, const id_t id) const override
	{
		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		PODArray<UInt64> ids{1, id};
		ColumnString out;
		getItems(attribute, ids, &out);

        return String{out.getDataAt(0)};
	};

#define DECLARE_MULTIPLE_GETTER(TYPE)\
	void get##TYPE(const std::string & attribute_name, const PODArray<id_t> & ids, PODArray<TYPE> & out) const override\
	{\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, ids, out);\
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
	void getString(const std::string & attribute_name, const PODArray<id_t> & ids, ColumnString * out) const override
	{
		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				"Type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems(attribute, ids, out);
	}

private:
	struct cell_metadata_t final
	{
		std::uint64_t id;
		std::chrono::system_clock::time_point expires_at;
	};

	struct attribute_t final
	{
		AttributeUnderlyingType type;
		std::tuple<UInt8, UInt16, UInt32, UInt64,
			Int8, Int16, Int32, Int64,
			Float32, Float64,
			String> null_values;
		std::tuple<std::unique_ptr<UInt8[]>,
			std::unique_ptr<UInt16[]>,
			std::unique_ptr<UInt32[]>,
			std::unique_ptr<UInt64[]>,
			std::unique_ptr<Int8[]>,
			std::unique_ptr<Int16[]>,
			std::unique_ptr<Int32[]>,
			std::unique_ptr<Int64[]>,
			std::unique_ptr<Float32[]>,
			std::unique_ptr<Float64[]>,
			std::unique_ptr<StringRef[]>> arrays;
	};

	void createAttributes()
	{
		const auto size = dict_struct.attributes.size();
		attributes.reserve(size);

		bytes_allocated += size * sizeof(cell_metadata_t);
		bytes_allocated += size * sizeof(attributes.front());

		for (const auto & attribute : dict_struct.attributes)
		{
			attribute_index_by_name.emplace(attribute.name, attributes.size());
			attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

			if (attribute.hierarchical)
			{
				hierarchical_attribute = &attributes.back();

				if (hierarchical_attribute->type != AttributeUnderlyingType::UInt64)
					throw Exception{
						"Hierarchical attribute must be UInt64.",
						ErrorCodes::TYPE_MISMATCH
					};
			}
		}
	}

	attribute_t createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
	{
		attribute_t attr{type};

		switch (type)
		{
			case AttributeUnderlyingType::UInt8:
				std::get<UInt8>(attr.null_values) = null_value.get<UInt64>();
				std::get<std::unique_ptr<UInt8[]>>(attr.arrays) = std::make_unique<UInt8[]>(size);
				bytes_allocated += size * sizeof(UInt8);
				break;
			case AttributeUnderlyingType::UInt16:
				std::get<UInt16>(attr.null_values) = null_value.get<UInt64>();
				std::get<std::unique_ptr<UInt16[]>>(attr.arrays) = std::make_unique<UInt16[]>(size);
				bytes_allocated += size * sizeof(UInt16);
				break;
			case AttributeUnderlyingType::UInt32:
				std::get<UInt32>(attr.null_values) = null_value.get<UInt64>();
				std::get<std::unique_ptr<UInt32[]>>(attr.arrays) = std::make_unique<UInt32[]>(size);
				bytes_allocated += size * sizeof(UInt32);
				break;
			case AttributeUnderlyingType::UInt64:
				std::get<UInt64>(attr.null_values) = null_value.get<UInt64>();
				std::get<std::unique_ptr<UInt64[]>>(attr.arrays) = std::make_unique<UInt64[]>(size);
				bytes_allocated += size * sizeof(UInt64);
				break;
			case AttributeUnderlyingType::Int8:
				std::get<Int8>(attr.null_values) = null_value.get<Int64>();
				std::get<std::unique_ptr<Int8[]>>(attr.arrays) = std::make_unique<Int8[]>(size);
				bytes_allocated += size * sizeof(Int8);
				break;
			case AttributeUnderlyingType::Int16:
				std::get<Int16>(attr.null_values) = null_value.get<Int64>();
				std::get<std::unique_ptr<Int16[]>>(attr.arrays) = std::make_unique<Int16[]>(size);
				bytes_allocated += size * sizeof(Int16);
				break;
			case AttributeUnderlyingType::Int32:
				std::get<Int32>(attr.null_values) = null_value.get<Int64>();
				std::get<std::unique_ptr<Int32[]>>(attr.arrays) = std::make_unique<Int32[]>(size);
				bytes_allocated += size * sizeof(Int32);
				break;
			case AttributeUnderlyingType::Int64:
				std::get<Int64>(attr.null_values) = null_value.get<Int64>();
				std::get<std::unique_ptr<Int64[]>>(attr.arrays) = std::make_unique<Int64[]>(size);
				bytes_allocated += size * sizeof(Int64);
				break;
			case AttributeUnderlyingType::Float32:
				std::get<Float32>(attr.null_values) = null_value.get<Float64>();
				std::get<std::unique_ptr<Float32[]>>(attr.arrays) = std::make_unique<Float32[]>(size);
				bytes_allocated += size * sizeof(Float32);
				break;
			case AttributeUnderlyingType::Float64:
				std::get<Float64>(attr.null_values) = null_value.get<Float64>();
				std::get<std::unique_ptr<Float64[]>>(attr.arrays) = std::make_unique<Float64[]>(size);
				bytes_allocated += size * sizeof(Float64);
				break;
			case AttributeUnderlyingType::String:
				std::get<String>(attr.null_values) = null_value.get<String>();
				std::get<std::unique_ptr<StringRef[]>>(attr.arrays) = std::make_unique<StringRef[]>(size);
				bytes_allocated += size * sizeof(StringRef);
				break;
		}

		return attr;
	}

	template <typename T>
	void getItems(attribute_t & attribute, const PODArray<id_t> & ids, PODArray<T> & out) const
	{
		HashMap<id_t, std::vector<std::size_t>> outdated_ids;
		auto & attribute_array = std::get<std::unique_ptr<T[]>>(attribute.arrays);

		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, decide which ones require update
			for (const auto i : ext::range(0, ids.size()))
			{
				const auto id = ids[i];
				if (id == 0)
				{
					out[i] = std::get<T>(attribute.null_values);
					continue;
				}

				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				if (cell.id != id || cell.expires_at < now)
				{
					out[i] = std::get<T>(attribute.null_values);
					outdated_ids[id].push_back(i);
				}
				else
					out[i] = attribute_array[cell_idx];
			}
		}

		query_count.fetch_add(ids.size(), std::memory_order_relaxed);
		hit_count.fetch_add(ids.size() - outdated_ids.size(), std::memory_order_release);

		if (outdated_ids.empty())
			return;

		/// request new values
		std::vector<id_t> required_ids(outdated_ids.size());
		std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
			[] (auto & pair) { return pair.first; });

		update(required_ids, [&] (const auto id, const auto cell_idx) {
			const auto attribute_value = attribute_array[cell_idx];

			/// set missing values to out
			for (const auto out_idx : outdated_ids[id])
				out[out_idx] = attribute_value;
		});
	}

	void getItems(attribute_t & attribute, const PODArray<id_t> & ids, ColumnString * out) const
	{
		/// save on some allocations
		out->getOffsets().reserve(ids.size());

		auto & attribute_array = std::get<std::unique_ptr<StringRef[]>>(attribute.arrays);

		auto found_outdated_values = false;

		/// perform optimistic version, fallback to pessimistic if failed
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, discard on fail
			for (const auto i : ext::range(0, ids.size()))
			{
				const auto id = ids[i];
				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				if (cell.id != id || cell.expires_at < now)
				{
					found_outdated_values = true;
					break;
				}
				else
				{
					const auto string_ref = attribute_array[cell_idx];
					out->insertData(string_ref.data, string_ref.size);
				}
			}
		}

		/// optimistic code completed successfully
		if (!found_outdated_values)
		{
			query_count.fetch_add(ids.size(), std::memory_order_relaxed);
			hit_count.fetch_add(ids.size(), std::memory_order_release);
			return;
		}

		/// now onto the pessimistic one, discard possibly partial results from the optimistic path
		out->getChars().resize_assume_reserved(0);
		out->getOffsets().resize_assume_reserved(0);

		/// outdated ids joined number of times they've been requested
		HashMap<id_t, std::size_t> outdated_ids;
		/// we are going to store every string separately
		HashMap<id_t, String> map;

		std::size_t total_length = 0;
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			for (const auto i : ext::range(0, ids.size()))
			{
				const auto id = ids[i];
				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				if (cell.id != id || cell.expires_at < now)
					outdated_ids[id] += 1;
				else
				{
					const auto string_ref = attribute_array[cell_idx];
					map[id] = String{string_ref};
					total_length += string_ref.size + 1;
				}
			}
		}

		query_count.fetch_add(ids.size(), std::memory_order_relaxed);
		hit_count.fetch_add(ids.size() - outdated_ids.size(), std::memory_order_release);

		/// request new values
		if (!outdated_ids.empty())
		{
			std::vector<id_t> required_ids(outdated_ids.size());
			std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
				[] (auto & pair) { return pair.first; });

			update(required_ids, [&] (const auto id, const auto cell_idx) {
				const auto attribute_value = attribute_array[cell_idx];

				map[id] = String{attribute_value};
				total_length += (attribute_value.size + 1) * outdated_ids[id];
			});
		}

		out->getChars().reserve(total_length);

		for (const auto id : ids)
		{
			const auto it = map.find(id);
			const auto string = it != map.end() ? it->second : std::get<String>(attribute.null_values);
			out->insertData(string.data(), string.size());
		}
	}

	template <typename F>
	void update(const std::vector<id_t> ids, F && on_cell_updated) const
	{
		auto stream = source_ptr->loadIds(ids);
		stream->readPrefix();

		HashMap<UInt64, UInt8> remaining_ids{ids.size()};
		for (const auto id : ids)
			remaining_ids.insert({ id, 0 });

		std::uniform_int_distribution<std::uint64_t> distribution{
			dict_lifetime.min_sec,
			dict_lifetime.max_sec
		};

		const Poco::ScopedWriteRWLock write_lock{rw_lock};

		while (const auto block = stream->read())
		{
			const auto id_column = typeid_cast<const ColumnVector<UInt64> *>(block.getByPosition(0).column.get());
			if (!id_column)
				throw Exception{
					"Id column has type different from UInt64.",
					ErrorCodes::TYPE_MISMATCH
				};

			const auto & ids = id_column->getData();

			/// cache column pointers
			std::vector<const IColumn *> column_ptrs(attributes.size());
			for (const auto i : ext::range(0, attributes.size()))
				column_ptrs[i] = block.getByPosition(i + 1).column.get();

			for (const auto i : ext::range(0, ids.size()))
			{
				const auto id = ids[i];
				const auto cell_idx = getCellIdx(id);
				auto & cell = cells[cell_idx];

				for (const auto attribute_idx : ext::range(0, attributes.size()))
				{
					const auto & attribute_column = *column_ptrs[attribute_idx];
					auto & attribute = attributes[attribute_idx];

					setAttributeValue(attribute, cell_idx, attribute_column[i]);
				}

				/// if cell id is zero and zero does not map to this cell, then the cell is unused
				if (cell.id == 0 && cell_idx != zero_cell_idx)
					element_count.fetch_add(1, std::memory_order_relaxed);

				cell.id = id;
				if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
					cell.expires_at = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};
				else
					cell.expires_at = std::chrono::time_point<std::chrono::system_clock>::max();

				on_cell_updated(id, cell_idx);
				remaining_ids[id] = 1;
			}
		}

		stream->readSuffix();

		for (const auto id_found_pair : remaining_ids)
		{
			if (id_found_pair.second)
				continue;

			const auto id = id_found_pair.first;
			const auto cell_idx = getCellIdx(id);
			auto & cell = cells[cell_idx];

			for (auto & attribute : attributes)
				setDefaultAttributeValue(attribute, cell_idx);

			if (cell.id == 0 && cell_idx != zero_cell_idx)
				element_count.fetch_add(1, std::memory_order_relaxed);

			cell.id = id;
			if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
				cell.expires_at = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};
			else
				cell.expires_at = std::chrono::time_point<std::chrono::system_clock>::max();

			on_cell_updated(id, cell_idx);
		}
	}

	std::uint64_t getCellIdx(const id_t id) const
	{
		const auto hash = intHash64(id);
		const auto idx = hash & (size - 1);
		return idx;
	}

	void setDefaultAttributeValue(attribute_t & attribute, const id_t idx) const
	{
		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: std::get<std::unique_ptr<UInt8[]>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values); break;
			case AttributeUnderlyingType::UInt16: std::get<std::unique_ptr<UInt16[]>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values); break;
			case AttributeUnderlyingType::UInt32: std::get<std::unique_ptr<UInt32[]>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values); break;
			case AttributeUnderlyingType::UInt64: std::get<std::unique_ptr<UInt64[]>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values); break;
			case AttributeUnderlyingType::Int8: std::get<std::unique_ptr<Int8[]>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values); break;
			case AttributeUnderlyingType::Int16: std::get<std::unique_ptr<Int16[]>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values); break;
			case AttributeUnderlyingType::Int32: std::get<std::unique_ptr<Int32[]>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values); break;
			case AttributeUnderlyingType::Int64: std::get<std::unique_ptr<Int64[]>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values); break;
			case AttributeUnderlyingType::Float32: std::get<std::unique_ptr<Float32[]>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values); break;
			case AttributeUnderlyingType::Float64: std::get<std::unique_ptr<Float64[]>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values); break;
			case AttributeUnderlyingType::String:
			{
				const auto & null_value_ref = std::get<String>(attribute.null_values);
				auto & string_ref = std::get<std::unique_ptr<StringRef[]>>(attribute.arrays)[idx];
				if (string_ref.data == null_value_ref.data())
					return;

				if (string_ref.size != 0)
					bytes_allocated -= string_ref.size + 1;
				const std::unique_ptr<const char[]> deleter{string_ref.data};

				string_ref = StringRef{null_value_ref};

				break;
			}
		}
	}

	void setAttributeValue(attribute_t & attribute, const id_t idx, const Field & value) const
	{
		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: std::get<std::unique_ptr<UInt8[]>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::UInt16: std::get<std::unique_ptr<UInt16[]>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::UInt32: std::get<std::unique_ptr<UInt32[]>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::UInt64: std::get<std::unique_ptr<UInt64[]>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::Int8: std::get<std::unique_ptr<Int8[]>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Int16: std::get<std::unique_ptr<Int16[]>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Int32: std::get<std::unique_ptr<Int32[]>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Int64: std::get<std::unique_ptr<Int64[]>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Float32: std::get<std::unique_ptr<Float32[]>>(attribute.arrays)[idx] = value.get<Float64>(); break;
			case AttributeUnderlyingType::Float64: std::get<std::unique_ptr<Float64[]>>(attribute.arrays)[idx] = value.get<Float64>(); break;
			case AttributeUnderlyingType::String:
			{
				const auto & string = value.get<String>();
				auto & string_ref = std::get<std::unique_ptr<StringRef[]>>(attribute.arrays)[idx];
				const auto & null_value_ref = std::get<String>(attribute.null_values);
				if (string_ref.data != null_value_ref.data())
				{
					if (string_ref.size != 0)
						bytes_allocated -= string_ref.size + 1;
					/// avoid explicit delete, let unique_ptr handle it
					const std::unique_ptr<const char[]> deleter{string_ref.data};
				}

				const auto size = string.size();
				if (size != 0)
				{
					auto string_ptr = std::make_unique<char[]>(size + 1);
					std::copy(string.data(), string.data() + size + 1, string_ptr.get());
					string_ref = StringRef{string_ptr.release(), size};
					bytes_allocated += size + 1;
				}
				else
					string_ref = {};

				break;
			}
		}
	}

	attribute_t & getAttribute(const std::string & attribute_name) const
	{
		const auto it = attribute_index_by_name.find(attribute_name);
		if (it == std::end(attribute_index_by_name))
			throw Exception{
				"No such attribute '" + attribute_name + "'",
				ErrorCodes::BAD_ARGUMENTS
			};

		return attributes[it->second];
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

	mutable Poco::RWLock rw_lock;
	const std::size_t size;
	const std::uint64_t zero_cell_idx{getCellIdx(0)};
	std::map<std::string, std::size_t> attribute_index_by_name;
	mutable std::vector<attribute_t> attributes;
	mutable std::vector<cell_metadata_t> cells;
	attribute_t * hierarchical_attribute = nullptr;

	mutable std::mt19937_64 rnd_engine{getSeed()};

	mutable std::size_t bytes_allocated = 0;
	mutable std::atomic<std::size_t> element_count{};
	mutable std::atomic<std::size_t> hit_count{};
	mutable std::atomic<std::size_t> query_count{};

	const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};

}
