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

	BOOST_STRONG_TYPEDEF(UInt16, Date)
	BOOST_STRONG_TYPEDEF(UInt32, DateTime)
	
	template <typename T> struct AggregateFunctionMedianTraits;
	
	template <> struct AggregateFunctionMedianTraits<Float64>
	{
		static DataTypePtr getReturnType() { return new DataTypeFloat64; }
		static void write(Float64 x, WriteBuffer & buf) { writeFloatBinary<Float64>(x, buf); }
		static void read(Float64 & x, ReadBuffer & buf) { readFloatBinary<Float64>(x, buf); }
		static bool validArgumentType(const DataTypePtr & argument) { return argument->isNumeric(); }
	};
	
	template <> struct AggregateFunctionMedianTraits<Date>
	{
		static DataTypePtr getReturnType() { return new DataTypeDate; }
		static void write(Date x, WriteBuffer & buf) { writeIntBinary<UInt16>(x, buf); }
		static void read(Date & x, ReadBuffer & buf) { readIntBinary<UInt16>(x, buf); }
		static bool validArgumentType(const DataTypePtr & argument) { return argument->getName() == "Date"; }
	};
	
	template <> struct AggregateFunctionMedianTraits<DateTime>
	{
		static DataTypePtr getReturnType() { return new DataTypeDateTime; }
		static void write(DateTime x, WriteBuffer & buf) { writeIntBinary<UInt32>(x, buf); }
		static void read(DateTime & x, ReadBuffer & buf) { readIntBinary<UInt32>(x, buf); }
		static bool validArgumentType(const DataTypePtr & argument) { return argument->getName() == "DateTime"; }
	};
	
	
	template <> struct TypeName<Date> 	{ static std::string get() { return "Date"; } };
	template <> struct TypeName<DateTime> 	{ static std::string get() { return "DateTime"; } };
	
	
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
			sample.writePOD(buf);
		}
		
		void deserializeMerge(ReadBuffer & buf)
		{
			ReservoirSampler<T> tmp_sample;
			tmp_sample.readPOD(buf);
			sample.merge(tmp_sample);
		}
		
		Field getResult() const
		{
			/// ReservoirSampler может отсортироваться при получении квантиля, но в этом контексте можно не считаеть это нарушением констанотности.
			return static_cast<T>(const_cast<ReservoirSampler<T>&>(sample).quantileInterpolated(0.5));
		}
	};
	
}
