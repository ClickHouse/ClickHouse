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

class RangeHashedDictionary final : public IDictionaryBase
{
public:
	RangeHashedDictionary(
		const std::string & name, const DictionaryStructure & dict_struct, DictionarySourcePtr source_ptr,
		const DictionaryLifetime dict_lifetime, bool require_nonempty)
		: name{name}, dict_struct(dict_struct),
		  source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
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

	RangeHashedDictionary(const RangeHashedDictionary & other)
		: RangeHashedDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
	{}

	std::exception_ptr getCreationException() const override { return creation_exception; }

	std::string getName() const override { return name; }

	std::string getTypeName() const override { return "RangeHashed"; }

	std::size_t getBytesAllocated() const override { return bytes_allocated; }

	std::size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

	double getHitRate() const override { return 1.0; }

	std::size_t getElementCount() const override { return element_count; }

	double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

	bool isCached() const override { return false; }

	DictionaryPtr clone() const override { return std::make_unique<RangeHashedDictionary>(*this); }

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
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const PaddedPODArray<UInt16> & dates,\
		PaddedPODArray<TYPE> & out) const\
	{\
		const auto & attribute = getAttributeWithType(attribute_name, AttributeUnderlyingType::TYPE);\
		getItems<TYPE>(attribute, ids, dates, out);\
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
		const std::string & attribute_name, const PaddedPODArray<id_t> & ids, const PaddedPODArray<UInt16> & dates,
		ColumnString * out) const
	{
		const auto & attribute = getAttributeWithType(attribute_name, AttributeUnderlyingType::String);
		const auto & attr = *std::get<ptr_t<StringRef>>(attribute.maps);
		const auto & null_value = std::get<String>(attribute.null_values);

		for (const auto i : ext::range(0, ids.size()))
		{
			const auto it = attr.find(ids[i]);
			if (it != std::end(attr))
			{
				const auto date = dates[i];
				const auto & ranges_and_values = it->second;
				const auto val_it = std::find_if(std::begin(ranges_and_values), std::end(ranges_and_values),
					[date] (const value_t<StringRef> & v) { return v.range.contains(date); });

				const auto string_ref = val_it != std::end(ranges_and_values) ? val_it->value : StringRef{null_value};
				out->insertData(string_ref.data, string_ref.size);
			}
			else
				out->insertData(null_value.data(), null_value.size());
		}

		query_count.fetch_add(ids.size(), std::memory_order_relaxed);
	}

private:
	struct range_t : std::pair<UInt16, UInt16>
	{
		using std::pair<UInt16, UInt16>::pair;

		bool contains(const UInt16 date) const
		{
			const auto & left = first;
			const auto & right = second;

			if (left <= date && date <= right)
				return true;

			const auto has_left_bound = 0 < left && left <= DATE_LUT_MAX_DAY_NUM;
			const auto has_right_bound = 0 < right && right <= DATE_LUT_MAX_DAY_NUM;

			if ((!has_left_bound || left <= date) && (!has_right_bound || date <= right))
				return true;

			return false;
		}
	};

	template <typename T>
	struct value_t final
	{
		range_t range;
		T value;
	};

	template <typename T> using values_t = std::vector<value_t<T>>;
	template <typename T> using collection_t = HashMap<UInt64, values_t<T>>;
	template <typename T> using ptr_t = std::unique_ptr<collection_t<T>>;

	struct attribute_t final
	{
	public:
		AttributeUnderlyingType type;
		std::tuple<UInt8, UInt16, UInt32, UInt64,
				   Int8, Int16, Int32, Int64,
				   Float32, Float64,
				   String> null_values;
		std::tuple<ptr_t<UInt8>, ptr_t<UInt16>, ptr_t<UInt32>, ptr_t<UInt64>,
				   ptr_t<Int8>, ptr_t<Int16>, ptr_t<Int32>, ptr_t<Int64>,
				   ptr_t<Float32>, ptr_t<Float64>, ptr_t<StringRef>> maps;
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
					name + ": hierarchical attributes not supported by " + getName() + " dictionary.",
					ErrorCodes::BAD_ARGUMENTS
				};
		}
	}

	void loadData()
	{
		auto stream = source_ptr->loadAll();
		stream->readPrefix();

		while (const auto block = stream->read())
		{
			const auto & id_column = *block.getByPosition(0).column;
			const auto & min_range_column = *block.getByPosition(1).column;
			const auto & max_range_column = *block.getByPosition(2).column;

			element_count += id_column.size();

			for (const auto attribute_idx : ext::range(0, attributes.size()))
			{
				const auto & attribute_column = *block.getByPosition(attribute_idx + 3).column;
				auto & attribute = attributes[attribute_idx];

				for (const auto row_idx : ext::range(0, id_column.size()))
					setAttributeValue(attribute, id_column[row_idx].get<UInt64>(),
						range_t(min_range_column[row_idx].get<UInt64>(), max_range_column[row_idx].get<UInt64>()),
						attribute_column[row_idx]);
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
		const auto & map_ref = std::get<ptr_t<T>>(attribute.maps);
		bytes_allocated += sizeof(collection_t<T>) + map_ref->getBufferSizeInBytes();
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
		std::get<ptr_t<T>>(attribute.maps) = std::make_unique<collection_t<T>>();
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
				std::get<ptr_t<StringRef>>(attr.maps) = std::make_unique<collection_t<StringRef>>();
				attr.string_arena = std::make_unique<Arena>();
				break;
			}
		}

		return attr;
	}

	template <typename T>
	void getItems(
		const attribute_t & attribute, const PaddedPODArray<id_t> & ids, const PaddedPODArray<UInt16> & dates,
		PaddedPODArray<T> & out) const
	{
		const auto & attr = *std::get<ptr_t<T>>(attribute.maps);
		const auto null_value = std::get<T>(attribute.null_values);

		for (const auto i : ext::range(0, ids.size()))
		{
			const auto it = attr.find(ids[i]);
			if (it != std::end(attr))
			{
				const auto date = dates[i];
				const auto & ranges_and_values = it->second;
				const auto val_it = std::find_if(std::begin(ranges_and_values), std::end(ranges_and_values),
					[date] (const value_t<T> & v) { return v.range.contains(date); });

				out[i] = val_it != std::end(ranges_and_values) ? val_it->value : null_value;
			}
			else
				out[i] = null_value;
		}

		query_count.fetch_add(ids.size(), std::memory_order_relaxed);
	}

	template <typename T>
	void setAttributeValueImpl(attribute_t & attribute, const id_t id, const range_t & range, const T value)
	{
		auto & map = *std::get<ptr_t<T>>(attribute.maps);
		const auto it = map.find(id);

		if (it != map.end())
		{
			auto & values = it->second;

			const auto insert_it = std::lower_bound(std::begin(values), std::end(values), range,
				[] (const value_t<T> & lhs, const range_t & range) {
					return lhs.range < range;
				});

			values.insert(insert_it, value_t<T>{ range, value });
		}
		else
			map.insert({ id, values_t<T>{ value_t<T>{ range, value } } });
	}

	void setAttributeValue(attribute_t & attribute, const id_t id, const range_t & range, const Field & value)
	{
		switch (attribute.type)
		{
			case AttributeUnderlyingType::UInt8: setAttributeValueImpl<UInt8>(attribute, id, range, value.get<UInt64>()); break;
			case AttributeUnderlyingType::UInt16: setAttributeValueImpl<UInt16>(attribute, id, range, value.get<UInt64>()); break;
			case AttributeUnderlyingType::UInt32: setAttributeValueImpl<UInt32>(attribute, id, range, value.get<UInt64>()); break;
			case AttributeUnderlyingType::UInt64: setAttributeValueImpl<UInt64>(attribute, id, range, value.get<UInt64>()); break;
			case AttributeUnderlyingType::Int8: setAttributeValueImpl<Int8>(attribute, id, range, value.get<Int64>()); break;
			case AttributeUnderlyingType::Int16: setAttributeValueImpl<Int16>(attribute, id, range, value.get<Int64>()); break;
			case AttributeUnderlyingType::Int32: setAttributeValueImpl<Int32>(attribute, id, range, value.get<Int64>()); break;
			case AttributeUnderlyingType::Int64: setAttributeValueImpl<Int64>(attribute, id, range, value.get<Int64>()); break;
			case AttributeUnderlyingType::Float32: setAttributeValueImpl<Float32>(attribute, id, range, value.get<Float64>()); break;
			case AttributeUnderlyingType::Float64: setAttributeValueImpl<Float64>(attribute, id, range, value.get<Float64>()); break;
			case AttributeUnderlyingType::String:
			{
				auto & map = *std::get<ptr_t<StringRef>>(attribute.maps);
				const auto & string = value.get<String>();
				const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
				const StringRef string_ref{string_in_arena, string.size()};

				const auto it = map.find(id);

				if (it != map.end())
				{
					auto & values = it->second;

					const auto insert_it = std::lower_bound(std::begin(values), std::end(values), range,
						[] (const value_t<StringRef> & lhs, const range_t & range) {
							return lhs.range < range;
						});

					values.insert(insert_it, value_t<StringRef>{ range, string_ref });
				}
				else
					map.insert({ id, values_t<StringRef>{ value_t<StringRef>{ range, string_ref } } });

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

	const attribute_t & getAttributeWithType(const std::string & name, const AttributeUnderlyingType type) const
	{
		const auto & attribute = getAttribute(name);
		if (attribute.type != type)
			throw Exception{
				name + ": type mismatch: attribute " + name + " has type " + toString(attribute.type),
				ErrorCodes::TYPE_MISMATCH
			};

		return attribute;
	}

	const std::string name;
	const DictionaryStructure dict_struct;
	const DictionarySourcePtr source_ptr;
	const DictionaryLifetime dict_lifetime;
	const bool require_nonempty;

	std::map<std::string, std::size_t> attribute_index_by_name;
	std::vector<attribute_t> attributes;

	std::size_t bytes_allocated = 0;
	std::size_t element_count = 0;
	std::size_t bucket_count = 0;
	mutable std::atomic<std::size_t> query_count{0};

	std::chrono::time_point<std::chrono::system_clock> creation_time;

	std::exception_ptr creation_exception;
};

}
