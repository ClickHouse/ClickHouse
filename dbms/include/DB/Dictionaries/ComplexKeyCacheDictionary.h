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
#include <random>


namespace DB
{


class ComplexKeyCacheDictionary final : public IDictionaryBase
{
public:
	ComplexKeyCacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
		DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
		const std::size_t size);

	ComplexKeyCacheDictionary(const ComplexKeyCacheDictionary & other);

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
		PaddedPODArray<TYPE> & out) const;
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
		ColumnString * out) const;

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		const PaddedPODArray<TYPE> & def, PaddedPODArray<TYPE> & out) const;
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
		const ColumnString * const def, ColumnString * const out) const;

#define DECLARE(TYPE)\
	void get##TYPE(\
		const std::string & attribute_name, const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types,\
		const TYPE def, PaddedPODArray<TYPE> & out) const;
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
		const String & def, ColumnString * const out) const;

	void has(const ConstColumnPlainPtrs & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

private:
	template <typename Value> using MapType = HashMapWithSavedHash<StringRef, Value, StringRefHash>;
	template <typename Value> using ContainerType = Value[];
	template <typename Value> using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

	struct CellMetadata final
	{
		using time_point_t = std::chrono::system_clock::time_point;
		using time_point_rep_t = time_point_t::rep;
		using time_point_urep_t = std::make_unsigned_t<time_point_rep_t>;

		static constexpr UInt64 EXPIRES_AT_MASK = std::numeric_limits<time_point_rep_t>::max();
		static constexpr UInt64 IS_DEFAULT_MASK = ~EXPIRES_AT_MASK;

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

	struct Attribute final
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

	void createAttributes();

	Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);

	template <typename OutputType, typename DefaultGetter>
	void getItemsNumber(
		Attribute & attribute,
		const ConstColumnPlainPtrs & key_columns,
		PaddedPODArray<OutputType> & out,
		DefaultGetter && get_default) const;

	template <typename AttributeType, typename OutputType, typename DefaultGetter>
	void getItemsNumberImpl(
		Attribute & attribute,
		const ConstColumnPlainPtrs & key_columns,
		PaddedPODArray<OutputType> & out,
		DefaultGetter && get_default) const;

	template <typename DefaultGetter>
	void getItemsString(
		Attribute & attribute, const ConstColumnPlainPtrs & key_columns, ColumnString * out,
		DefaultGetter && get_default) const;

	template <typename PresentKeyHandler, typename AbsentKeyHandler>
	void update(
		const ConstColumnPlainPtrs & in_key_columns, const PODArray<StringRef> & in_keys,
		const std::vector<std::size_t> & in_requested_rows, PresentKeyHandler && on_cell_updated,
		AbsentKeyHandler && on_key_not_found) const;

	UInt64 getCellIdx(const StringRef key) const;

	void setDefaultAttributeValue(Attribute & attribute, const std::size_t idx) const;

	void setAttributeValue(Attribute & attribute, const std::size_t idx, const Field & value) const;

	Attribute & getAttribute(const std::string & attribute_name) const;

	StringRef allocKey(const std::size_t row, const ConstColumnPlainPtrs & key_columns, StringRefs & keys) const;

	void freeKey(const StringRef key) const;

	template <typename Arena>
	static StringRef placeKeysInPool(
		const std::size_t row, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool);

	StringRef placeKeysInFixedSizePool(
		const std::size_t row, const ConstColumnPlainPtrs & key_columns) const;

	static StringRef copyIntoArena(StringRef src, Arena & arena);
	StringRef copyKey(const StringRef key) const;

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;
	const std::string key_description{dict_struct.getKeyDescription()};

	mutable Poco::RWLock rw_lock;
	const std::size_t size;
	const UInt64 zero_cell_idx{getCellIdx(StringRef{})};
	std::map<std::string, std::size_t> attribute_index_by_name;
	mutable std::vector<Attribute> attributes;
	mutable std::vector<CellMetadata> cells{size};
	const bool key_size_is_fixed{dict_struct.isKeySizeFixed()};
	std::size_t key_size{key_size_is_fixed ? dict_struct.getKeySize() : 0};
	std::unique_ptr<ArenaWithFreeLists> keys_pool = key_size_is_fixed ? nullptr :
		std::make_unique<ArenaWithFreeLists>();
	std::unique_ptr<SmallObjectPool> fixed_size_keys_pool = key_size_is_fixed ?
		std::make_unique<SmallObjectPool>(key_size) : nullptr;
	std::unique_ptr<ArenaWithFreeLists> string_arena;

	mutable std::mt19937_64 rnd_engine;

	mutable std::size_t bytes_allocated = 0;
	mutable std::atomic<std::size_t> element_count{0};
	mutable std::atomic<std::size_t> hit_count{0};
	mutable std::atomic<std::size_t> query_count{0};

	const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};

}
