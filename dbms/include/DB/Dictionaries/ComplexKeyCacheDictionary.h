#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Common/Arena.h>
#include <DB/Common/ArenaWithFreeLists.h>
#include <DB/Common/SmallObjectPool.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Core/StringRef.h>
#include <ext/enumerate.hpp>
#include <ext/scope_guard.hpp>
#include <ext/bit_cast.hpp>
#include <ext/range.hpp>
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


class ComplexKeyCacheDictionary final : public IDictionaryBase
{
public:
	ComplexKeyCacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
		const std::size_t size)
		: name{name}, dict_struct(dict_struct), source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
		  size{round_up_to_power_of_two(size)}
	{
		if (!this->source_ptr->supportsSelectiveLoad())
			throw Exception{
				name + ": source cannot be used with ComplexKeyCacheDictionary",
				ErrorCodes::UNSUPPORTED_METHOD
			};

		createAttributes();
	}

	ComplexKeyCacheDictionary(const ComplexKeyCacheDictionary & other)
		: ComplexKeyCacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
	{}

	std::string getKeyDescription() const { return key_description; };

	std::exception_ptr getCreationException() const override { return {}; }

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "ComplexKeyCache"; }

	std::size_t getBytesAllocated() const override
	{
		return bytes_allocated + (key_size_is_fixed ? fixed_size_keys_pool->size() : keys_pool->size()) +
			(string_arena ? string_arena->size() : 0);
	}

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

	DictionaryPtr clone() const override { return std::make_unique<ComplexKeyCacheDictionary>(*this); }

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

	/// Во всех функциях ниже, key_columns должны быть полноценными (не константными) столбцами.
	/// См. требование в IDataType.h для функций текстовой сериализации.
#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		PaddedPODArray<TYPE> & out) const\
	{\
		dict_struct.validateKeyTypes(key_types);\
		\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		const auto null_value = std::get<TYPE>(attribute.null_values);\
		\
		getItems<TYPE>(attribute, key_columns, out, [&] (const std::size_t) { return null_value; });\
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
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
		ColumnString * out) const
	{
		dict_struct.validateKeyTypes(key_types);

		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		const auto null_value = StringRef{std::get<String>(attribute.null_values)};

		getItems(attribute, key_columns, out, [&] (const std::size_t) { return null_value; });
	}

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		const PaddedPODArray<TYPE> & def, PaddedPODArray<TYPE> & out) const\
	{\
		dict_struct.validateKeyTypes(key_types);\
		\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, key_columns, out, [&] (const std::size_t row) { return def[row]; });\
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
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
		const ColumnString * const def, ColumnString * const out) const
	{
		dict_struct.validateKeyTypes(key_types);

		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems(attribute, key_columns, out, [&] (const std::size_t row) { return def->getDataAt(row); });
	}

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		const TYPE def, PaddedPODArray<TYPE> & out) const\
	{\
		dict_struct.validateKeyTypes(key_types);\
		\
		auto & attribute = getAttribute(attribute_name);\
		if (attribute.type != AttributeUnderlyingType::TYPE)\
			throw Exception{\
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
				ErrorCodes::TYPE_MISMATCH\
			};\
		\
		getItems<TYPE>(attribute, key_columns, out, [&] (const std::size_t) { return def; });\
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
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,
		const String & def, ColumnString * const out) const
	{
		dict_struct.validateKeyTypes(key_types);

		auto & attribute = getAttribute(attribute_name);
		if (attribute.type != AttributeUnderlyingType::String)
			throw Exception{
				name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		getItems(attribute, key_columns, out, [&] (const std::size_t) { return StringRef{def}; });
	}

	void has(const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
	{
		dict_struct.validateKeyTypes(key_types);

		/// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
		MapType<std::vector<std::size_t>> outdated_keys;

		const auto rows = key_columns.front()->size();
		const auto keys_size = dict_struct.key.value().size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;
		PODArray<StringRef> keys_array(rows);

		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, decide which ones require update
			for (const auto row : ext::range(0, rows))
			{
				const auto key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
				keys_array[row] = key;
				const auto hash = StringRefHash{}(key);
				const auto cell_idx = hash & (size - 1);
				const auto & cell = cells[cell_idx];

				/** cell should be updated if either:
				 *	1. keys (or hash) do not match,
				 *	2. cell has expired,
				 *	3. explicit defaults were specified and cell was set default. */
				if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
					outdated_keys[key].push_back(row);
				else
					out[row] = !cell.isDefault();
			}
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows - outdated_keys.size(), std::memory_order_release);

		if (outdated_keys.empty())
			return;

		std::vector<std::size_t> required_rows(outdated_keys.size());
		std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows),
			[] (auto & pair) { return pair.second.front(); });

		/// request new values
		update(key_columns, keys_array, required_rows, [&] (const auto key, const auto) {
			for (const auto out_idx : outdated_keys[key])
				out[out_idx] = true;
		}, [&] (const auto key, const auto) {
			for (const auto out_idx : outdated_keys[key])
				out[out_idx] = false;
		});
	}

private:
	template <typename Value> using MapType = HashMapWithSavedHash<StringRef, Value, StringRefHash>;
	template <typename Value> using ContainerType = Value[];
	template <typename Value> using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

	struct cell_metadata_t final
	{
		using time_point_t = std::chrono::system_clock::time_point;
		using time_point_rep_t = time_point_t::rep;
		using time_point_urep_t = std::make_unsigned_t<time_point_rep_t>;

		static constexpr std::uint64_t EXPIRES_AT_MASK = std::numeric_limits<time_point_rep_t>::max();
		static constexpr std::uint64_t IS_DEFAULT_MASK = ~EXPIRES_AT_MASK;

		StringRef key;
		decltype(StringRefHash{}(key)) hash;
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
				throw Exception{
					name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
					ErrorCodes::TYPE_MISMATCH
				};
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
		attribute_t & attribute, const ConstColumnPlainPtrs & key_columns, PaddedPODArray<T> & out,
		DefaultGetter && get_default) const
	{
		/// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
		MapType<std::vector<std::size_t>> outdated_keys;
		auto & attribute_array = std::get<ContainerPtrType<T>>(attribute.arrays);

		const auto rows = key_columns.front()->size();
		const auto keys_size = dict_struct.key.value().size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;
		PODArray<StringRef> keys_array(rows);

		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, decide which ones require update
			for (const auto row : ext::range(0, rows))
			{
				const auto key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
				keys_array[row] = key;
				const auto hash = StringRefHash{}(key);
				const auto cell_idx = hash & (size - 1);
				const auto & cell = cells[cell_idx];

				/** cell should be updated if either:
				 *	1. keys (or hash) do not match,
				 *	2. cell has expired,
				 *	3. explicit defaults were specified and cell was set default. */
				if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
					outdated_keys[key].push_back(row);
				else
					out[row] =  cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
			}
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows - outdated_keys.size(), std::memory_order_release);

		if (outdated_keys.empty())
			return;

		std::vector<std::size_t> required_rows(outdated_keys.size());
		std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows),
			[] (auto & pair) { return pair.second.front(); });

		/// request new values
		update(key_columns, keys_array, required_rows, [&] (const auto key, const auto cell_idx) {
			for (const auto row : outdated_keys[key])
				out[row] = attribute_array[cell_idx];
		}, [&] (const auto key, const auto cell_idx) {
			for (const auto row : outdated_keys[key])
				out[row] = get_default(row);
		});
	}

	template <typename DefaultGetter>
	void getItems(
		attribute_t & attribute, const ConstColumnPlainPtrs & key_columns, ColumnString * out,
		DefaultGetter && get_default) const
	{
		const auto rows = key_columns.front()->size();
		/// save on some allocations
		out->getOffsets().reserve(rows);

		const auto keys_size = dict_struct.key.value().size();
		StringRefs keys(keys_size);
		Arena temporary_keys_pool;

		auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

		auto found_outdated_values = false;

		/// perform optimistic version, fallback to pessimistic if failed
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			/// fetch up-to-date values, discard on fail
			for (const auto row : ext::range(0, rows))
			{
				const auto key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
				SCOPE_EXIT(temporary_keys_pool.rollback(key.size));
				const auto hash = StringRefHash{}(key);
				const auto cell_idx = hash & (size - 1);
				const auto & cell = cells[cell_idx];

				if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
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

		/// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
		MapType<std::vector<std::size_t>> outdated_keys;
		/// we are going to store every string separately
		MapType<String> map;
		PODArray<StringRef> keys_array(rows);

		std::size_t total_length = 0;
		{
			const Poco::ScopedReadRWLock read_lock{rw_lock};

			const auto now = std::chrono::system_clock::now();
			for (const auto row : ext::range(0, rows))
			{
				const auto key = placeKeysInPool(row, key_columns, keys, temporary_keys_pool);
				keys_array[row] = key;
				const auto hash = StringRefHash{}(key);
				const auto cell_idx = hash & (size - 1);
				const auto & cell = cells[cell_idx];

				if (cell.hash != hash || cell.key != key || cell.expiresAt() < now)
					outdated_keys[key].push_back(row);
				else
				{
					const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

					if (!cell.isDefault())
						map[key] = String{string_ref};

					total_length += string_ref.size + 1;
				}
			}
		}

		query_count.fetch_add(rows, std::memory_order_relaxed);
		hit_count.fetch_add(rows - outdated_keys.size(), std::memory_order_release);

		/// request new values
		if (!outdated_keys.empty())
		{
			std::vector<std::size_t> required_rows(outdated_keys.size());
			std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows),
				[] (auto & pair) { return pair.second.front(); });

			update(key_columns, keys_array, required_rows, [&] (const auto key, const auto cell_idx) {
				const auto attribute_value = attribute_array[cell_idx];

				map[key] = String{attribute_value};
				total_length += (attribute_value.size + 1) * outdated_keys[key].size();
			}, [&] (const auto key, const auto cell_idx) {
				for (const auto row : outdated_keys[key])
					total_length += get_default(row).size + 1;
			});
		}

		out->getChars().reserve(total_length);

		for (const auto row : ext::range(0, ext::size(keys_array)))
		{
			const auto key = keys_array[row];
			const auto it = map.find(key);
			const auto string_ref = it != std::end(map) ? StringRef{it->second} : get_default(row);
			out->insertData(string_ref.data, string_ref.size);
		}
	}

	template <typename PresentKeyHandler, typename AbsentKeyHandler>
	void update(
		const ConstColumnPlainPtrs & in_key_columns, const PODArray<StringRef> & in_keys,
		const std::vector<std::size_t> & in_requested_rows, PresentKeyHandler && on_cell_updated,
		AbsentKeyHandler && on_key_not_found) const
	{
		MapType<bool> remaining_keys{in_requested_rows.size()};
		for (const auto row : in_requested_rows)
			remaining_keys.insert({ in_keys[row], false });

		std::uniform_int_distribution<std::uint64_t> distribution{
			dict_lifetime.min_sec,
			dict_lifetime.max_sec
		};

		const Poco::ScopedWriteRWLock write_lock{rw_lock};

		auto stream = source_ptr->loadKeys(in_key_columns, in_requested_rows);
		stream->readPrefix();

		const auto keys_size = dict_struct.key.value().size();
		StringRefs keys(keys_size);

		const auto attributes_size = attributes.size();

		while (const auto block = stream->read())
		{
			/// cache column pointers
			const auto key_columns = ext::map<ConstColumnPlainPtrs>(ext::range(0, keys_size),
				[&] (const std::size_t attribute_idx) {
					return block.getByPosition(attribute_idx).column.get();
				});

			const auto attribute_columns = ext::map<ConstColumnPlainPtrs>(ext::range(0, attributes_size),
				[&] (const std::size_t attribute_idx) {
					return block.getByPosition(keys_size + attribute_idx).column.get();
				});

			const auto rows = block.rowsInFirstColumn();

			for (const auto row : ext::range(0, rows))
			{
				auto key = allocKey(row, key_columns, keys);
				const auto hash = StringRefHash{}(key);
				const auto cell_idx = hash & (size - 1);
				auto & cell = cells[cell_idx];

				for (const auto attribute_idx : ext::range(0, attributes.size()))
				{
					const auto & attribute_column = *attribute_columns[attribute_idx];
					auto & attribute = attributes[attribute_idx];

					setAttributeValue(attribute, cell_idx, attribute_column[row]);
				}

				/// if cell id is zero and zero does not map to this cell, then the cell is unused
				if (cell.key == StringRef{} && cell_idx != zero_cell_idx)
					element_count.fetch_add(1, std::memory_order_relaxed);

				/// handle memory allocated for old key
				if (key == cell.key)
				{
					freeKey(key);
					key = cell.key;
				}
				else
				{
					/// new key is different from the old one
					if (cell.key.data)
						freeKey(cell.key);

					cell.key = key;
				}

				cell.hash = hash;

				if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
					cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
				else
					cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

				/// inform caller
				on_cell_updated(key, cell_idx);
				/// mark corresponding id as found
				remaining_keys[key] = true;
			}
		}

		stream->readSuffix();

		/// Check which ids have not been found and require setting null_value
		for (const auto key_found_pair : remaining_keys)
		{
			if (key_found_pair.second)
				continue;

			auto key = key_found_pair.first;
			const auto hash = StringRefHash{}(key);
			const auto cell_idx = hash & (size - 1);
			auto & cell = cells[cell_idx];

			/// Set null_value for each attribute
			for (auto & attribute : attributes)
				setDefaultAttributeValue(attribute, cell_idx);

			/// Check if cell had not been occupied before and increment element counter if it hadn't
			if (cell.key == StringRef{} && cell_idx != zero_cell_idx)
				element_count.fetch_add(1, std::memory_order_relaxed);

			if (key == cell.key)
				key = cell.key;
			else
			{
				if (cell.key.data)
					freeKey(cell.key);

				/// copy key from temporary pool
				key = copyKey(key);
				cell.key = key;
			}

			cell.hash = hash;

			if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
				cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
			else
				cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

			cell.setDefault();

			/// inform caller that the cell has not been found
			on_key_not_found(key, cell_idx);
		}
	}

	std::uint64_t getCellIdx(const StringRef key) const
	{
		const auto hash = StringRefHash{}(key);
		const auto idx = hash & (size - 1);
		return idx;
	}

	void setDefaultAttributeValue(attribute_t & attribute, const std::size_t idx) const
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

	void setAttributeValue(attribute_t & attribute, const std::size_t idx, const Field & value) const
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

	StringRef allocKey(const std::size_t row, const ConstColumnPlainPtrs & key_columns, StringRefs & keys) const
	{
		if (key_size_is_fixed)
			return placeKeysInFixedSizePool(row, key_columns);

		return placeKeysInPool(row, key_columns, keys, *keys_pool);
	}

	void freeKey(const StringRef key) const
	{
		if (key_size_is_fixed)
			fixed_size_keys_pool->free(const_cast<char *>(key.data));
		else
			keys_pool->free(const_cast<char *>(key.data), key.size);
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

	template <typename Arena>
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

	StringRef placeKeysInFixedSizePool(
		const std::size_t row, const ConstColumnPlainPtrs & key_columns) const
	{
		const auto res = fixed_size_keys_pool->alloc();
		auto place = res;

		for (const auto & key_column : key_columns)
		{
			const auto key = key_column->getDataAt(row);
			memcpy(place, key.data, key.size);
			place += key.size;
		}

		return { res, key_size };
	}

	StringRef copyKey(const StringRef key) const
	{
		const auto res = key_size_is_fixed ? fixed_size_keys_pool->alloc() : keys_pool->alloc(key.size);
		memcpy(res, key.data, key.size);

		return { res, key.size };
	}

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;
	const std::string key_description{dict_struct.getKeyDescription()};

	mutable Poco::RWLock rw_lock;
	const std::size_t size;
	const std::uint64_t zero_cell_idx{getCellIdx(StringRef{})};
	std::map<std::string, std::size_t> attribute_index_by_name;
	mutable std::vector<attribute_t> attributes;
	mutable std::vector<cell_metadata_t> cells{size};
	const bool key_size_is_fixed{dict_struct.isKeySizeFixed()};
	std::size_t key_size{key_size_is_fixed ? dict_struct.getKeySize() : 0};
	std::unique_ptr<ArenaWithFreeLists> keys_pool = key_size_is_fixed ? nullptr :
		std::make_unique<ArenaWithFreeLists>();
	std::unique_ptr<SmallObjectPool> fixed_size_keys_pool = key_size_is_fixed ?
		std::make_unique<SmallObjectPool>(key_size) : nullptr;
	std::unique_ptr<ArenaWithFreeLists> string_arena;

	mutable std::mt19937_64 rnd_engine{getSeed()};

	mutable std::size_t bytes_allocated = 0;
	mutable std::atomic<std::size_t> element_count{0};
	mutable std::atomic<std::size_t> hit_count{0};
	mutable std::atomic<std::size_t> query_count{0};

	const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};

}
