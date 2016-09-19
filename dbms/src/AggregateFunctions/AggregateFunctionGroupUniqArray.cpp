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

struct DataGroupUniqArray : public IAggregateDataWithArena
{
	using Set = HashSetWithSavedHash<StringRef, StringRefHash, HashTableGrower<4>, HashTableAllocatorWithStackMemory<16>>;
	Set value;
};


template <bool is_plain_column=false>
inline StringRef getSerialization(const IColumn & column, size_t row_num, Arena & arena)
{
	const char * begin = nullptr;
	return column.serializeValueIntoArena(row_num, arena, begin);
}

template <>
inline StringRef getSerialization<true>(const IColumn & column, size_t row_num, Arena & arena)
{
	return column.getDataAt(row_num);
}

template <bool is_plain_column=false>
inline void deserializeAndInsert(StringRef str, IColumn & data_to)
{
	data_to.deserializeAndInsertFromArena(str.data);
}

template <>
inline void deserializeAndInsert<true>(StringRef str, IColumn & data_to)
{
	data_to.insertData(str.data, str.size);
}


template <bool is_plain_column=false>
class AggreagteFunctionGroupUniqArrayGeneric : public IUnaryAggregateFunction<DataGroupUniqArray, AggreagteFunctionGroupUniqArrayGeneric<is_plain_column>>
{
	mutable DataTypePtr input_data_type;

	using State = DataGroupUniqArray;

public:

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

		auto & set = this->data(place).value;
		writeVarUInt(set.size(), buf);

		for (auto & elem: set)
		{
			writeStringBinary(elem, buf);
		}
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		throw Exception(ErrorCodes::NOT_IMPLEMENTED);

		State::Set & set = this->data(place).value;
		size_t size;
		readVarUInt(size, buf);
		//TODO: set.reserve(size);

		std::string str_buf;
		for (size_t i = 0; i < size; i++)
		{
			readStringBinary(str_buf, buf);
			set.insert(StringRef(str_buf));
		}
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		auto arena = this->data(place).arena;
		auto & set = this->data(place).value;

		bool inserted;
		State::Set::iterator it;

		StringRef str_serialized = getSerialization<is_plain_column>(column, row_num, *arena);
		set.emplace(str_serialized, it, inserted);

		if (!is_plain_column)
		{
			if (!likely(inserted))
				arena->rollback(str_serialized.size);
		}
		else
		{
			if (unlikely(inserted))
			{
				it->data = arena->insert(str_serialized.data, str_serialized.size);
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

		auto & set = this->data(place).value;
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + set.size());

		for (auto & elem : set)
		{
			deserializeAndInsert<is_plain_column>(elem, data_to);
		}
	}
};

}


namespace
{

static IAggregateFunction * createWithExtraTypes(const IDataType & argument_type)
{
		 if (typeid_cast<const DataTypeDateTime *>(&argument_type))	return new AggregateFunctionGroupUniqArray<UInt32>;
	else if (typeid_cast<const DataTypeDate *>(&argument_type)) 	return new AggregateFunctionGroupUniqArray<UInt16>;
	else
	{
		/// Check that we can use plain version of AggreagteFunctionGroupUniqArrayGeneric

		if (typeid_cast<const DataTypeString*>(&argument_type))
			return new AggreagteFunctionGroupUniqArrayGeneric<true>;

		auto * array_type = typeid_cast<const DataTypeArray *>(&argument_type);
		if (array_type)
		{
			auto nested_type = array_type->getNestedType();
			if (nested_type->isNumeric() || typeid_cast<DataTypeFixedString *>(nested_type.get()))
				return new AggreagteFunctionGroupUniqArrayGeneric<true>;
		}

		return new AggreagteFunctionGroupUniqArrayGeneric<false>;
	}
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
