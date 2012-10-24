#pragma once

#include <stats/ReservoirSampler.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

	/// Приближённо вычисляет медиану.
	template <typename T>
	class AggregateFunctionMedian : public IUnaryAggregateFunction
	{
	private:
		ReservoirSampler<T> sample;
		DataTypePtr type;
		
	public:
		AggregateFunctionMedian() {}
		AggregateFunctionMedian(DataTypePtr type_) : type(type_) {}
		
		String getName() const { return "median"; }
		String getTypeID() const { return "median_" + type->getName(); }
		
		AggregateFunctionPlainPtr cloneEmpty() const
		{
			AggregateFunctionMedian<T> * res = new AggregateFunctionMedian<T>;
			res->type = type;
			return res;
		}
		
		DataTypePtr getReturnType() const
		{
			return type;
		}
		
		void setArgument(const DataTypePtr & argument)
		{
			if (type->getName() != argument->getName())
				throw Exception("Argument type mismatch", ErrorCodes::TYPE_MISMATCH);
		}
		
		void addOne(const Field & value)
		{
			sample.insert(boost::get<T>(value));
		}
		
		void merge(const IAggregateFunction & rhs)
		{
			sample.merge(static_cast<const AggregateFunctionMedian<T> &>(rhs).sample);
		}
		
		void serialize(WriteBuffer & buf) const
		{
			sample.write(buf);
		}
		
		void deserializeMerge(ReadBuffer & buf)
		{
			ReservoirSampler<T> tmp_sample;
			tmp_sample.read(buf);
			sample.merge(tmp_sample);
		}
		
		Field getResult() const
		{
			/// ReservoirSampler может отсортироваться при получении квантиля, но в этом контексте можно не считаеть это нарушением констанотности.
			return static_cast<T>(const_cast<ReservoirSampler<T>&>(sample).quantileInterpolated(0.5));
		}
	};
	
}
