#include <DB/Functions/Conditional/getArrayType.h>
#include <DB/Functions/Conditional/CondException.h>
#include <DB/Functions/Conditional/common.h>
#include <DB/Functions/DataTypeTraits.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace Conditional
{

namespace
{

std::string dumpArgTypes(const DataTypes & args)
{
	std::string out;
	WriteBufferFromString buf{out};

	bool is_first = true;
	for (size_t i = 0; i < args.size(); ++i)
	{
		if (is_first)
			is_first = false;
		else
			writeString("; ", buf);

		writeString(args[i]->getName(), buf);
	}

	buf.next();

	return out;
}

/// Forward declarations.
template <typename TResult, typename TType>
class ResultDataTypeDeducer;

/// Internal class used by ResultDataTypeDeducer. Calls ResultDataTypeDeducer
/// for the next element to be processed.
template <typename TType>
class ResultDataTypeDeducerImpl final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		if (! (ResultDataTypeDeducer<TType, DataTypeUInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeUInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeUInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeUInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeFloat32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeFloat64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeNull>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeUInt8> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeUInt16> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeUInt32> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeUInt64> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeInt8> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeInt16> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeInt32> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeInt64> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeFloat32> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, Nullable<DataTypeFloat64> >::execute(args, i, type_res)))
			throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};
	}
};

/// Specialization for the error type.
template <>
class ResultDataTypeDeducerImpl<typename NumberTraits::Error> final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw CondException{CondErrorCodes::TYPE_DEDUCER_UPSCALING_ERROR, dump};
	}
};

template <typename TType>
struct TypeChecker
{
	static bool execute(const DataTypePtr & arg)
	{
		if (arg->isNullable())
			return false;
		return typeid_cast<const TType *>(arg.get()) != nullptr;
	}
};

template <typename TType>
struct TypeChecker<Nullable<TType>>
{
	static bool execute(const DataTypePtr & arg)
	{
		if (!arg->isNullable())
			return false;

		const DataTypeNullable & nullable_type = static_cast<DataTypeNullable &>(*arg);
		const IDataType * nested_type = nullable_type.getNestedType().get();
		return typeid_cast<const TType *>(nested_type) != nullptr;
	}
};

/// Analyze the type of the element currently being processed of an array.
/// Subsequently perform the same analysis for the remaining elements.
/// Determine the returned type if all the processed elements are numeric.
template <typename TResult, typename TType>
class ResultDataTypeDeducer final
{
private:
	using TCombined = typename DataTypeTraits::DataTypeProduct<TResult, TType>::Type;
	using DataTypeDeducerImpl = ResultDataTypeDeducerImpl<TCombined>;

public:
	static bool execute(const DataTypes & args, size_t i, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		if (!TypeChecker<TType>::execute(args[i]))
			return false;

		if (i == (args.size() - 1))
		{
			type_res = DataTypeTraits::ToEnrichedDataTypeObject<TCombined, false>::execute();
			if ((type_res.first == DataTypePtr()) && (type_res.second == DataTypePtr()))
				throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};
		}
		else
		{
			++i;
			DataTypeDeducerImpl::execute(args, i, type_res);
		}

		return true;
	}
};

/// Analyze the type of each element of an array.
/// Determine the returned type if all elements are numeric.
class FirstResultDataTypeDeducer final
{
public:
	static void execute(const DataTypes & args, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		using Void = typename DataTypeTraits::ToEnrichedDataType<
			NumberTraits::Enriched::Void<NumberTraits::HasNoNull>
		>::Type;

		size_t i = 0;

		if (! (ResultDataTypeDeducer<Void, DataTypeUInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeUInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeUInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeUInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeFloat32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeFloat64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeNull>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeUInt8> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeUInt16> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeUInt32> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeUInt64> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeInt8> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeInt16> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeInt32> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeInt64> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeFloat32> >::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, Nullable<DataTypeFloat64> >::execute(args, i, type_res)))
			throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};
	}
};

}

DataTypeTraits::EnrichedDataTypePtr getArrayType(const DataTypes & args)
{
	DataTypeTraits::EnrichedDataTypePtr type_res;
	FirstResultDataTypeDeducer::execute(args, type_res);
	return type_res;
}

}

}
