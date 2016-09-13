#include <DB/Common/PODArray.h>
#include <DB/Core/StringRef.h>
#include <DB/Core/FieldVisitors.h>
#include <common/logger_useful.h>
#include <set>
#include <unordered_set>

#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/UInt128.h>
#include <DB/Common/Arena.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <DB/AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

template <bool>
class Function_DataNaive;

template <bool>
class Function_Fair;

template <bool>
class Function_Map128;


template <bool is_string_column=false>
//using Function_Default = Function_Map128<is_string_column>;
using Function_Default = Function_Fair<is_string_column>;

struct DataNaive : public IAggregateDataWithArena
{
	using Set = std::unordered_set<std::string>;
	Set value;
};

struct DataFair : public IAggregateDataWithArena
{
	using Set = HashSetWithSavedHash<StringRef, StringRefHash, HashTableGrower<4>, HashTableAllocatorWithStackMemory<16>>;
	Set value;
};

struct DataMap128 : public IAggregateDataWithArena
{
	using Set = HashMap<UInt128, StringRef, UInt128HashCRC32, HashTableGrower<4>, HashTableAllocatorWithStackMemory<16>>;
	Set value;

#ifdef AVERAGE_STAT
	mutable bool was_merged = false;
#endif
};



template <bool is_string_column=false>
inline StringRef packToStringRef(const IColumn & column, size_t row_num, Arena & arena)
{
	const char * begin = nullptr;
	return column.serializeValueIntoArena(row_num, arena, begin);
}

template <>
inline StringRef packToStringRef<true>(const IColumn & column, size_t row_num, Arena & arena)
{
	StringRef str = column.getDataAt(row_num);
	str.data = arena.insert(str.data, str.size);
	return str;
}

template <bool is_string_column=false>
inline void unpackFromStringRef(StringRef str, IColumn & data_to)
{
	data_to.deserializeAndInsertFromArena(str.data);
}

template <>
inline void unpackFromStringRef<true>(StringRef str, IColumn & data_to)
{
	data_to.insertData(str.data, str.size);
}


template <typename Data, typename Derived>
class IAggreagteFunctionGroupUniqArray : public IUnaryAggregateFunction<Data, Derived>
{
protected:

	mutable DataTypePtr input_data_type;

public:

	using State = Data;

	String getName() const override { return "groupUniqArray"; }

	void setArgument(const DataTypePtr & argument)
	{
		input_data_type = argument;
	}

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeArray>(input_data_type->clone());
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		throw Exception(ErrorCodes::NOT_IMPLEMENTED);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		throw Exception(ErrorCodes::NOT_IMPLEMENTED);
	}

	void destroy(AggregateDataPtr place) const noexcept override
	{
		this->data(place).~Data();
		//LOG_DEBUG(&Logger::get("IAggreagteFunctionGroupUniqArray"), "destroyed.");
	}
};

template <bool is_string_column=false>
struct Function_DataNaive : public IAggreagteFunctionGroupUniqArray<DataNaive, Function_DataNaive<is_string_column>>
{
	using State = DataNaive;

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		StringRef serialized_elem;
		if (is_string_column)
			serialized_elem = static_cast<const ColumnString &>(column).getDataAt(row_num);
		else
			serialized_elem = packToStringRef<is_string_column>(column, row_num, *this->data(place).arena);

		this->data(place).value.insert(serialized_elem.toString());
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		State::Set & cur_set = this->data(place).value;
		const State::Set & rhs_set = this->data(rhs).value;

		for (auto it = begin(rhs_set); it != end(rhs_set); ++it)
		{
			cur_set.insert(*it);
		}
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();
		IColumn & data_to = arr_to.getData();

		const State::Set & set = this->data(place).value;
		size_t size = set.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		for (auto it = begin(set); it != end(set); ++it)
		{
			unpackFromStringRef<is_string_column>(*it, data_to);
		}
	}
};

template <bool is_string_column=false>
struct Function_Fair : public IAggreagteFunctionGroupUniqArray<DataFair, Function_Fair<is_string_column>>
{
	using State = DataFair;

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		if (!is_string_column)
		{
			StringRef serialized_elem = packToStringRef<is_string_column>(column, row_num, *this->data(place).arena);

			bool inserted;
			State::Set::iterator it;
			this->data(place).value.emplace(serialized_elem, it, inserted);

			if (!likely(inserted))
				this->data(place).arena->rollback(serialized_elem.size);
		}
		else
		{
			StringRef str = column.getDataAt(row_num);

			bool inserted;
			State::Set::iterator it;
			this->data(place).value.emplace(str, it, inserted);

			if (unlikely(inserted))
			{
				it->data = this->data(place).arena->insert(str.data, str.size);
			}
		}
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		State::Set & cur_set = this->data(place).value;
		const State::Set & rhs_set = this->data(rhs).value;
		cur_set.merge(rhs_set);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();
		IColumn & data_to = arr_to.getData();

		const State::Set & set = this->data(place).value;
		size_t size = set.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		for (auto it = set.begin(); it != set.end(); ++it)
		{
			unpackFromStringRef<is_string_column>(*it, data_to);
		}
	}
};

template <bool is_string_column=false>
struct Function_Map128 : public IAggreagteFunctionGroupUniqArray<DataMap128, Function_Map128<is_string_column>>
{
	using State = DataMap128;

#ifdef AVERAGE_STAT
	mutable long sum_size_on_result = 0, sum_size_before_merge = 0;
	mutable long cnt_on_result = 0, cnt_before_merge = 0;
#endif

	inline static UInt128 getUInt128Descriptor(StringRef elem_serialized)
	{
		UInt128 elem_desc;
		SipHash hasher;
		hasher.update(elem_serialized.data, elem_serialized.size);
		hasher.get128(elem_desc.first, elem_desc.second);
		return elem_desc;
	}

	void addImpl_ownHashing(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		StringRef elem_serialized;
		if (is_string_column)
			elem_serialized = column.getDataAt(row_num);
		else
			elem_serialized = packToStringRef<is_string_column>(column, row_num, *this->data(place).arena);

		auto elem_desc = getUInt128Descriptor(elem_serialized);

		bool inserted;
		State::Set::iterator it;
		this->data(place).value.emplace(elem_desc, it, inserted);

		if (likely(inserted))
		{
			if (is_string_column)
				elem_serialized.data = this->data(place).arena->insert(elem_serialized.data, elem_serialized.size);
			it->second = elem_serialized;
		}
		else
		{
			/** The probability that two different elems has the same UInt128 descriptor is extremely slow.
			  * Don't handle this case. Free arena's memory, there are no need to store duplicates.
			  */
			if (!is_string_column)
				this->data(place).arena->rollback(elem_serialized.size);
		}
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		SipHash hasher;
		column.updateHashWithValue(row_num, hasher);

		UInt128 elem_desc;
		hasher.get128(elem_desc.first, elem_desc.second);

		bool inserted;
		State::Set::iterator it;
		this->data(place).value.emplace(elem_desc, it, inserted);

		if (likely(inserted))
		{
			Arena & arena = *this->data(place).arena;
			StringRef & elem_serialization = it->second;

			if (is_string_column)
			{
				elem_serialization = column.getDataAt(row_num);
				elem_serialization.data = arena.insert(elem_serialization.data, elem_serialization.size);
			}
			else
			{
				const char * ptr = nullptr;
				elem_serialization = column.serializeValueIntoArena(row_num, arena, ptr);
			}
		}
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		State::Set & cur_set = this->data(place).value;
		const State::Set & rhs_set = this->data(rhs).value;

		bool inserted;
		State::Set::iterator it_cur;
		for (auto it_rhs = rhs_set.begin(); it_rhs != rhs_set.end(); ++it_rhs)
		{
			cur_set.emplace(it_rhs->first, it_cur, inserted);
			if (inserted)
				it_cur->second = it_rhs->second;
		}

#ifdef AVERAGE_STAT
		if (!this->data(rhs).was_merged)
		{
			sum_size_before_merge += rhs_set.size();
			cnt_before_merge += 1;
		}

		this->data(place).was_merged = true;
		this->data(rhs).was_merged = true;
#endif
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();
		IColumn & data_to = arr_to.getData();

		const State::Set & set = this->data(place).value;
		size_t size = set.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		for (auto && value : set)
		{
			unpackFromStringRef<is_string_column>(value.second, data_to);
		}

#ifdef AVERAGE_STAT
		sum_size_on_result += set.size();
		cnt_on_result += 1;

		LOG_DEBUG(&Logger::get("IAggreagteFunctionGroupUniqArray"), "sum_size_on_result=" << sum_size_on_result << ", cnt_on_result=" << cnt_on_result << ", average=" << sum_size_on_result / cnt_on_result);
		LOG_DEBUG(&Logger::get("IAggreagteFunctionGroupUniqArray"), "sum_size_before_merge=" << sum_size_before_merge << ", cnt_before_merge=" << cnt_before_merge << ", average=" << sum_size_before_merge / cnt_before_merge);
#endif
	}
};

}


namespace
{

static IAggregateFunction * createWithExtraTypes(const IDataType & argument_type)
{
		 if (typeid_cast<const DataTypeDateTime *>(&argument_type))	return new AggregateFunctionGroupUniqArray<UInt32>;
	else if (typeid_cast<const DataTypeDate *>(&argument_type)) 	return new AggregateFunctionGroupUniqArray<UInt16>;
 	else if (typeid_cast<const DataTypeString*>(&argument_type))
 	{
		return new Function_Default<true>;
 	}
	else
	{
		return new Function_Default<>;
	}

	//return nullptr;
}

AggregateFunctionPtr createAggregateFunctionGroupUniqArray(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupUniqArray>(*argument_types[0]));

	if (!res)
		res = AggregateFunctionPtr(createWithExtraTypes(*argument_types[0]));

	if (!res)
		throw Exception("Illegal type " + argument_types[0]->getName() +
			" of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return res;
}

}

void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory)
{
	factory.registerFunction("groupUniqArray", createAggregateFunctionGroupUniqArray);
}

}
