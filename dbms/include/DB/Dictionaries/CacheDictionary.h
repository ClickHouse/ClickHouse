#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/ArenaWithFreeLists.h>
#include <DB/Columns/ColumnString.h>
#include <ext/scope_guard.hpp>
#include <ext/bit_cast.hpp>
#include <ext/range.hpp>
#include <ext/size.hpp>
#include <ext/map.hpp>
#include <Poco/RWLock.h>
#include <cmath>
#include <atomic>
#include <chrono>
#include <vector>
#include <map>
#include <tuple>


namespace DB
{

namespace ErrorCodes
{
	extern const int TYPE_MISMATCH;
	extern const int BAD_ARGUMENTS;
	extern const int UNSUPPORTED_METHOD;
}


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
				name + ": source cannot be used with CacheDictionary",
				ErrorCodes::UNSUPPORTED_METHOD
			};

		createAttributes();
	}

	CacheDictionary(const CacheDictionary & other)
		: CacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
	{}

	std::exception_ptr getCreationException() const override { return {}; }

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "Cache"; }

	std::size_t getBytesAllocated() const override { return bytes_allocated + (string_arena ? string_arena->size() : 0); }

	std::size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

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

	bool isInjective(const std::string & attribute_name) const override
	{
		return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
	}

	bool hasHierarchy() const override { return hierarchical_attribute; }

	void toParent(const PaddedPODArray<id_t> & ids, PaddedPODArray<id_t> & out) const override
	{
		const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

		getItems<UInt64>(*hierarchical_attribute, ids, out, [&] (const std::size_t) { return null_value; });
	}

#define DECLARE(TYPE)\
	void get##TYPE(const std::string & attribute_name, const PaddedPODArray<id_t> & ids, PaddedPODArray<TYPE> & out) const\
	{\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		const auto null_value = std::get<TYPE>(attribute.null_values);\
		\
		getItems<TYPE>(attribute, ids, out, [&] (const std::size_t) { return null_value; });\
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
		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		const auto null_value = StringRef{std::get<String>(attribute.null_values)};

		getItems(attribute, ids, out, [&] (const std::size_t) { return null_value; });
	}

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const PaddedPODArray<TYPE> & def,\
		PaddedPODArray<TYPE> & out) const\
	{\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, ids, out, [&] (const std::size_t row) { return def[row]; });\
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
		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems(attribute, ids, out, [&] (const std::size_t row) { return def->getDataAt(row); });
	}

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const TYPE def, PaddedPODArray<TYPE> & out) const\
	{\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, ids, out, [&] (const std::size_t) { return def; });\
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
		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems(attribute, ids, out, [&] (const std::size_t) { return StringRef{def}; });
	}

	void has(const PaddedPODArray<id_t> & ids, PaddedPODArray<UInt8> & out) const override
	{
		/// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
		MapType<std::vector<std::size_t>> outdated_ids;

		const auto rows = ext::size(ids);
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, decide which ones require update
			for (const auto row : ext::range(0, rows))
			{
				const auto id = ids[row];
				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				/** cell should be updated if either:
				 *	1. ids do not match,
				 *	2. cell has expired,
				 *	3. explicit defaults were specified and cell was set default. */
				if (cell.id != id || cell.expiresAt() < now)
					outdated_ids[id].push_back(row);
				else
					out[row] = !cell.isDefault();
			}
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

		if (outdated_ids.empty())
			return;

		std::vector<id_t> required_ids(outdated_ids.size());
		std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
			[] (auto & pair) { return pair.first; });

		/// request new values
		update(required_ids, [&] (const auto id, const auto) {
			for (const auto row : outdated_ids[id])
				out[row] = true;
		}, [&] (const auto id, const auto) {
			for (const auto row : outdated_ids[id])
				out[row] = false;
		});
	}

private:
	template <typename Value> using MapType = HashMap<id_t, Value>;
	template <typename Value> using ContainerType = Value[];
	template <typename Value> using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

	struct cell_metadata_t final
	{
		using time_point_t = std::chrono::system_clock::time_point;
		using time_point_rep_t = time_point_t::rep;
		using time_point_urep_t = std::make_unsigned_t<time_point_rep_t>;

		static constexpr std::uint64_t EXPIRES_AT_MASK = std::numeric_limits<time_point_rep_t>::max();
		static constexpr std::uint64_t IS_DEFAULT_MASK = ~EXPIRES_AT_MASK;

		std::uint64_t id;
		/// Stores both expiration time and `is_default` flag in the most significant bit
		time_point_urep_t data;

		/// Sets expiration time, resets `is_default` flag to false
		time_point_t expiresAt() const { return ext::safe_bit_cast<time_point_t>(data & EXPIRES_AT_MASK); }
		void setExpiresAt(const time_point_t & t) { data = ext::safe_bit_cast<time_point_urep_t>(t); }

		bool isDefault() const { return (data & IS_DEFAULT_MASK) == IS_DEFAULT_MASK; }
		void setDefault() { data |= IS_DEFAULT_MASK; }
	};

	struct attribute_t final
	{
		AttributeUnderlyingType type;
		std::tuple<
			UInt8, UInt16, UInt32, UInt64,
			Int8, Int16, Int32, Int64,
			Float32, Float64,
			String> null_values;
		std::tuple<
			ContainerPtrType<UInt8>, ContainerPtrType<UInt16>, ContainerPtrType<UInt32>, ContainerPtrType<UInt64>,
			ContainerPtrType<Int8>, ContainerPtrType<Int16>, ContainerPtrType<Int32>, ContainerPtrType<Int64>,
			ContainerPtrType<Float32>, ContainerPtrType<Float64>,
			ContainerPtrType<StringRef>> arrays;
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
						name + ": hierarchical attribute must be UInt64.",
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
				std::get<ContainerPtrType<UInt8>>(attr.arrays) = std::make_unique<ContainerType<UInt8>>(size);
				bytes_allocated += size * sizeof(UInt8);
				break;
			case AttributeUnderlyingType::UInt16:
				std::get<UInt16>(attr.null_values) = null_value.get<UInt64>();
				std::get<ContainerPtrType<UInt16>>(attr.arrays) = std::make_unique<ContainerType<UInt16>>(size);
				bytes_allocated += size * sizeof(UInt16);
				break;
			case AttributeUnderlyingType::UInt32:
				std::get<UInt32>(attr.null_values) = null_value.get<UInt64>();
				std::get<ContainerPtrType<UInt32>>(attr.arrays) = std::make_unique<ContainerType<UInt32>>(size);
				bytes_allocated += size * sizeof(UInt32);
				break;
			case AttributeUnderlyingType::UInt64:
				std::get<UInt64>(attr.null_values) = null_value.get<UInt64>();
				std::get<ContainerPtrType<UInt64>>(attr.arrays) = std::make_unique<ContainerType<UInt64>>(size);
				bytes_allocated += size * sizeof(UInt64);
				break;
			case AttributeUnderlyingType::Int8:
				std::get<Int8>(attr.null_values) = null_value.get<Int64>();
				std::get<ContainerPtrType<Int8>>(attr.arrays) = std::make_unique<ContainerType<Int8>>(size);
				bytes_allocated += size * sizeof(Int8);
				break;
			case AttributeUnderlyingType::Int16:
				std::get<Int16>(attr.null_values) = null_value.get<Int64>();
				std::get<ContainerPtrType<Int16>>(attr.arrays) = std::make_unique<ContainerType<Int16>>(size);
				bytes_allocated += size * sizeof(Int16);
				break;
			case AttributeUnderlyingType::Int32:
				std::get<Int32>(attr.null_values) = null_value.get<Int64>();
				std::get<ContainerPtrType<Int32>>(attr.arrays) = std::make_unique<ContainerType<Int32>>(size);
				bytes_allocated += size * sizeof(Int32);
				break;
			case AttributeUnderlyingType::Int64:
				std::get<Int64>(attr.null_values) = null_value.get<Int64>();
				std::get<ContainerPtrType<Int64>>(attr.arrays) = std::make_unique<ContainerType<Int64>>(size);
				bytes_allocated += size * sizeof(Int64);
				break;
			case AttributeUnderlyingType::Float32:
				std::get<Float32>(attr.null_values) = null_value.get<Float64>();
				std::get<ContainerPtrType<Float32>>(attr.arrays) = std::make_unique<ContainerType<Float32>>(size);
				bytes_allocated += size * sizeof(Float32);
				break;
			case AttributeUnderlyingType::Float64:
				std::get<Float64>(attr.null_values) = null_value.get<Float64>();
				std::get<ContainerPtrType<Float64>>(attr.arrays) = std::make_unique<ContainerType<Float64>>(size);
				bytes_allocated += size * sizeof(Float64);
				break;
			case AttributeUnderlyingType::String:
				std::get<String>(attr.null_values) = null_value.get<String>();
				std::get<ContainerPtrType<StringRef>>(attr.arrays) = std::make_unique<ContainerType<StringRef>>(size);
				bytes_allocated += size * sizeof(StringRef);
				if (!string_arena)
					string_arena = std::make_unique<ArenaWithFreeLists>();
				break;
		}

		return attr;
	}

	template <typename T, typename DefaultGetter>
	void getItems(
		attribute_t & attribute, const PaddedPODArray<id_t> & ids, PaddedPODArray<T> & out, DefaultGetter && get_default) const
	{
		/// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
		MapType<std::vector<std::size_t>> outdated_ids;
		auto & attribute_array = std::get<ContainerPtrType<T>>(attribute.arrays);
		const auto rows = ext::size(ids);

		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, decide which ones require update
			for (const auto row : ext::range(0, rows))
			{
				const auto id = ids[row];
				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				/** cell should be updated if either:
				 *	1. ids do not match,
				 *	2. cell has expired,
				 *	3. explicit defaults were specified and cell was set default. */
				if (cell.id != id || cell.expiresAt() < now)
					outdated_ids[id].push_back(row);
				else
					out[row] = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
			}
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

		if (outdated_ids.empty())
			return;

		std::vector<id_t> required_ids(outdated_ids.size());
		std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
			[] (auto & pair) { return pair.first; });

		/// request new values
		update(required_ids, [&] (const auto id, const auto cell_idx) {
			const auto attribute_value = attribute_array[cell_idx];

			for (const auto row : outdated_ids[id])
				out[row] = attribute_value;
		}, [&] (const auto id, const auto cell_idx) {
			for (const auto row : outdated_ids[id])
				out[row] = get_default(row);
		});
	}

	template <typename DefaultGetter>
	void getItems(
		attribute_t & attribute, const PaddedPODArray<id_t> & ids, ColumnString * out, DefaultGetter && get_default) const
	{
		const auto rows = ext::size(ids);

		/// save on some allocations
		out->getOffsets().reserve(rows);

		auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

		auto found_outdated_values = false;

		/// perform optimistic version, fallback to pessimistic if failed
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, discard on fail
			for (const auto row : ext::range(0, rows))
			{
				const auto id = ids[row];
				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				if (cell.id != id || cell.expiresAt() < now)
				{
					found_outdated_values = true;
					break;
				}
				else
				{
					const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
					out->insertData(string_ref.data, string_ref.size);
				}
			}
		}

		/// optimistic code completed successfully
		if (!found_outdated_values)
		{
			query_count.fetch_add(rows, std::memory_order_relaxed);
			hit_count.fetch_add(rows, std::memory_order_release);
			return;
		}

		/// now onto the pessimistic one, discard possible partial results from the optimistic path
		out->getChars().resize_assume_reserved(0);
		out->getOffsets().resize_assume_reserved(0);

		/// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
		MapType<std::vector<std::size_t>> outdated_ids;
		/// we are going to store every string separately
		MapType<String> map;

		std::size_t total_length = 0;
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			for (const auto row : ext::range(0, ids.size()))
			{
				const auto id = ids[row];
				const auto cell_idx = getCellIdx(id);
				const auto & cell = cells[cell_idx];

				if (cell.id != id || cell.expiresAt() < now)
					outdated_ids[id].push_back(row);
				else
				{
					const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

					if (!cell.isDefault())
						map[id] = String{string_ref};

					total_length += string_ref.size + 1;
				}
			}
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

		/// request new values
		if (!outdated_ids.empty())
		{
			std::vector<id_t> required_ids(outdated_ids.size());
			std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
				[] (auto & pair) { return pair.first; });

			update(required_ids, [&] (const auto id, const auto cell_idx) {
				const auto attribute_value = attribute_array[cell_idx];

				map[id] = String{attribute_value};
				total_length += (attribute_value.size + 1) * outdated_ids[id].size();
			}, [&] (const auto id, const auto cell_idx) {
				for (const auto row : outdated_ids[id])
					total_length += get_default(row).size + 1;
			});
		}

		out->getChars().reserve(total_length);

		for (const auto row : ext::range(0, ext::size(ids)))
		{
			const auto id = ids[row];
			const auto it = map.find(id);

			const auto string_ref = it != std::end(map) ? StringRef{it->second} : get_default(row);
			out->insertData(string_ref.data, string_ref.size);
		}
	}

	template <typename PresentIdHandler, typename AbsentIdHandler>
	void update(
		const std::vector<id_t> & requested_ids, PresentIdHandler && on_cell_updated,
		AbsentIdHandler && on_id_not_found) const
	{
		MapType<UInt8> remaining_ids{requested_ids.size()};
		for (const auto id : requested_ids)
			remaining_ids.insert({ id, 0 });

		std::uniform_int_distribution<std::uint64_t> distribution{
			dict_lifetime.min_sec,
			dict_lifetime.max_sec
		};

		const Poco::ScopedWriteRWLock write_lock{rw_lock};

		auto stream = source_ptr->loadIds(requested_ids);
		stream->readPrefix();

		while (const auto block = stream->read())
		{
			const auto id_column = typeid_cast<const ColumnVector<UInt64> *>(block.getByPosition(0).column.get());
			if (!id_column)
				throw Exception{
					name + ": id column has type different from UInt64.",
					ErrorCodes::TYPE_MISMATCH
				};

			const auto & ids = id_column->getData();

			/// cache column pointers
			const auto column_ptrs = ext::map<std::vector>(ext::range(0, attributes.size()), [&block] (const auto & i) {
				return block.getByPosition(i + 1).column.get();
			});

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
					cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
				else
					cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

				/// inform caller
				on_cell_updated(id, cell_idx);
				/// mark corresponding id as found
				remaining_ids[id] = 1;
			}
		}

		stream->readSuffix();

		/// Check which ids have not been found and require setting null_value
		for (const auto id_found_pair : remaining_ids)
		{
			if (id_found_pair.second)
				continue;

			const auto id = id_found_pair.first;
			const auto cell_idx = getCellIdx(id);
			auto & cell = cells[cell_idx];

			/// Set null_value for each attribute
			for (auto & attribute : attributes)
				setDefaultAttributeValue(attribute, cell_idx);

			/// Check if cell had not been occupied before and increment element counter if it hadn't
			if (cell.id == 0 && cell_idx != zero_cell_idx)
				element_count.fetch_add(1, std::memory_order_relaxed);

			cell.id = id;
			if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
				cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
			else
				cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

			cell.setDefault();

			/// inform caller that the cell has not been found
			on_id_not_found(id, cell_idx);
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
			case AttributeUnderlyingType::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values); break;
			case AttributeUnderlyingType::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values); break;
			case AttributeUnderlyingType::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values); break;
			case AttributeUnderlyingType::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values); break;
			case AttributeUnderlyingType::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values); break;
			case AttributeUnderlyingType::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values); break;
			case AttributeUnderlyingType::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values); break;
			case AttributeUnderlyingType::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values); break;
			case AttributeUnderlyingType::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values); break;
			case AttributeUnderlyingType::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values); break;
			case AttributeUnderlyingType::String:
			{
				const auto & null_value_ref = std::get<String>(attribute.null_values);
				auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];

				if (string_ref.data != null_value_ref.data())
				{
					if (string_ref.data)
						string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

					string_ref = StringRef{null_value_ref};
				}

				break;
			}
		}
	}

	void setAttributeValue(attribute_t & attribute, const id_t idx, const Field & value) const
	{
		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
			case AttributeUnderlyingType::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>(); break;
			case AttributeUnderlyingType::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>(); break;
			case AttributeUnderlyingType::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>(); break;
			case AttributeUnderlyingType::String:
			{
				const auto & string = value.get<String>();
				auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];
				const auto & null_value_ref = std::get<String>(attribute.null_values);

				/// free memory unless it points to a null_value
				if (string_ref.data && string_ref.data != null_value_ref.data())
					string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

				const auto size = string.size();
				if (size != 0)
				{
					auto string_ptr = string_arena->alloc(size + 1);
					std::copy(string.data(), string.data() + size + 1, string_ptr);
					string_ref = StringRef{string_ptr, size};
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
				name + ": no such attribute '" + attribute_name + "'",
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
	std::unique_ptr<ArenaWithFreeLists> string_arena;

	mutable std::mt19937_64 rnd_engine{getSeed()};

	mutable std::size_t bytes_allocated = 0;
	mutable std::atomic<std::size_t> element_count{0};
	mutable std::atomic<std::size_t> hit_count{0};
	mutable std::atomic<std::size_t> query_count{0};

	const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};

}
