#pragma once

#include <city.h>

#include <stats/UniquesHashSet.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


template <typename T> struct AggregateFunctionUniqTraits;

template <> struct AggregateFunctionUniqTraits<UInt64>
{
	static UInt64 hash(UInt64 x) { return x; }
};

template <> struct AggregateFunctionUniqTraits<Int64>
{
	static UInt64 hash(Int64 x) { return x; }
};

template <> struct AggregateFunctionUniqTraits<Float64>
{
	static UInt64 hash(Float64 x)
	{
		UInt64 res = 0;
		memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
		return res;
	}
};

template <> struct AggregateFunctionUniqTraits<String>
{
	/// Имейте ввиду, что вычисление приближённое.
	static UInt64 hash(const String & x) { return CityHash64(x.data(), x.size()); }
};


/// Приближённо вычисляет количество различных значений.
template <typename T>
class AggregateFunctionUniq : public IUnaryAggregateFunction
{
private:
	UniquesHashSet set;
	
public:
	AggregateFunctionUniq() {}

	String getName() const { return "uniq"; }
	String getTypeID() const { return "uniq_" + TypeName<T>::get(); }

	AggregateFunctionPlainPtr cloneEmpty() const
	{
		return new AggregateFunctionUniq<T>;
	}
	
	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt64;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void addOne(const Field & value)
	{
		set.insert(AggregateFunctionUniqTraits<T>::hash(get<const T &>(value)));
	}

	void merge(const IAggregateFunction & rhs)
	{
		set.merge(static_cast<const AggregateFunctionUniq<T> &>(rhs).set);
	}

	void serialize(WriteBuffer & buf) const
	{
		set.write(buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		UniquesHashSet tmp_set;
		tmp_set.read(buf);
		set.merge(tmp_set);
	}

	Field getResult() const
	{
		return set.size();
	}
};

}
