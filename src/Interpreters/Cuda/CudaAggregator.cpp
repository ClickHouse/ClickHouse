#include <algorithm>
#include <future>
#include <numeric>
#include <Poco/Util/Application.h>

#include <AggregateFunctions/AggregateFunctionArray.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/NativeWriter.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/JIT/compileFunction.h>
#include <base/sort.h>
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/LRUCache.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTSelectQuery.h>

#include <AggregateFunctions/AggregateFunctionCount.h>

#include <Interpreters/Cuda/CudaAggregator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CUDA_UNSUPPORTED_CASE;
}

/// For now leave it that way
Block CudaAggregator::getHeader(bool final) const
{
    Block res;

    if (params.src_header)
    {
        for (size_t i = 0; i < params.keys_size; ++i)
            res.insert(params.src_header.safeGetByPosition(params.keys[i]).cloneEmpty());

        for (size_t i = 0; i < params.aggregates_size; ++i)
        {
            size_t arguments_size = params.aggregates[i].arguments.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = params.src_header.safeGetByPosition(params.aggregates[i].arguments[j]).type;

            DataTypePtr type;
            if (final)
                type = params.aggregates[i].function->getReturnType();
            else
                type = std::make_shared<DataTypeAggregateFunction>(
                    params.aggregates[i].function, argument_types, params.aggregates[i].parameters);

            res.insert({type, params.aggregates[i].column_name});
        }
    }
    else if (params.intermediate_header)
    {
        res = params.intermediate_header.cloneEmpty();

        if (final)
        {
            for (size_t i = 0; i < params.aggregates_size; ++i)
            {
                auto & elem = res.getByPosition(params.keys_size + i);

                elem.type = params.aggregates[i].function->getReturnType();
                elem.column = elem.type->createColumn();
            }
        }
    }

    return materializeBlock(res);
}


CudaAggregator::CudaAggregator(ContextPtr context_, const Params & params_) : context(context_), params(params_)
{
    /// Here we cut off unsupported cases

    if (params.keys_size != 1)
        throw Exception("CudaAggregator: params.keys_size is not equal 1", ErrorCodes::CUDA_UNSUPPORTED_CASE);
    if (params.aggregates_size != 1)
        throw Exception("CudaAggregator: params.aggregates_size is not equal 1", ErrorCodes::CUDA_UNSUPPORTED_CASE);

    const auto & key_pos = params.keys[0];
    const auto & key_type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(key_pos).type;

    if (WhichDataType(key_type).isNullable())
        throw Exception("CudaAggregator: have no idea what is nullable key", ErrorCodes::CUDA_UNSUPPORTED_CASE);
    if (!WhichDataType(key_type).isString())
        throw Exception("CudaAggregator: key is not String", ErrorCodes::CUDA_UNSUPPORTED_CASE);

    /// Throws an exception if function CUDA version is not implemented
    cuda_agg_function = params.aggregates[0].function->createCudaFunction();

    if (params.aggregates[0].arguments.size() != 1)
        throw Exception("CudaAggregator: arguments number of function is not equal 1", ErrorCodes::CUDA_UNSUPPORTED_CASE);

    const auto & arg_pos = params.aggregates[0].arguments[0];
    const auto & arg_type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(arg_pos).type;

    if (WhichDataType(arg_type).isNullable())
        throw Exception("CudaAggregator: have no idea what is nullable argument", ErrorCodes::CUDA_UNSUPPORTED_CASE);
    if (!WhichDataType(arg_type).isString())
        throw Exception("CudaAggregator: argument is not String", ErrorCodes::CUDA_UNSUPPORTED_CASE);
}


bool CudaAggregator::executeOnBlock(
    Columns columns,
    UInt64 num_rows,
    CudaAggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
    bool & /*no_more_keys*/) const
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i].resize(params.aggregates[i].arguments.size());

    /** Constant columns are not supported directly during aggregation.
      * To make them work anyway, we materialize them.
      */
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
    {
        key_columns[i] = columns[params.keys[i]].get();

        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            materialized_columns.push_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            aggregate_columns[i][j] = columns[params.aggregates[i].arguments[j]].get();

            if (ColumnPtr converted = aggregate_columns[i][j]->convertToFullColumnIfConst())
            {
                materialized_columns.push_back(converted);
                aggregate_columns[i][j] = materialized_columns.back().get();
            }
        }
    }

    result.start(context, cuda_agg_function);
    // if (result.empty())
    // {
    //     result.init(context, cuda_agg_function);
    //     result.startProcessing();
    // }

    /// TODO get rid of this const_cast (problems is getChars and getOffsets been nonconst methods)
    ColumnString *keys_column = const_cast<ColumnString *>(static_cast<const ColumnString *>(key_columns[0])),
                 *vals_column = const_cast<ColumnString *>(static_cast<const ColumnString *>(aggregate_columns[0][0]));

    // const Settings & settings = context->getSettingsRef();
    result.strings_agg->queueData(
        num_rows,
        keys_column->getChars().size(),
        reinterpret_cast<const char *>(keys_column->getChars().data()),
        keys_column->getOffsets().data(),
        vals_column->getChars().size(),
        reinterpret_cast<const char *>(vals_column->getChars().data()),
        vals_column->getOffsets().data());
    result.strings_agg->waitQueueData();

    return true;
}

void NO_INLINE CudaAggregator::convertToBlockImplFinal(
    CudaAggregatedDataVariants & data_variants, MutableColumns & key_columns, MutableColumns & final_aggregate_columns) const
{
    for (const auto & elem : data_variants.strings_agg->getResult())
    {
        key_columns[0]->insertData(elem.first.c_str(), elem.first.length());

        /// TODO we must have special interface for this insertion
        UInt64 res = cuda_agg_function->getResult(elem.second);
        static_cast<ColumnUInt64 &>(*final_aggregate_columns[0]).getData().push_back(res);
    }
}


template <typename Filler>
Block CudaAggregator::prepareBlockAndFill(CudaAggregatedDataVariants & /*data_variants*/, bool final, size_t rows, Filler && filler) const
{
    MutableColumns key_columns(params.keys_size);
    MutableColumns final_aggregate_columns(params.aggregates_size);

    Block header = getHeader(final);

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        key_columns[i] = header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        final_aggregate_columns[i] = params.aggregates[i].function->getReturnType()->createColumn();
        final_aggregate_columns[i]->reserve(rows);
    }

    filler(key_columns, final_aggregate_columns);

    Block res = header.cloneEmpty();

    for (size_t i = 0; i < params.keys_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        res.getByPosition(i + params.keys_size).column = std::move(final_aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = header.columns();
    for (size_t i = 0; i < columns; ++i)
        if (isColumnConst(*(res.getByPosition(i).column)))
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}


Block CudaAggregator::prepareBlockAndFillSingleLevel(CudaAggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = data_variants.strings_agg->getResult().size();

    auto filler = [&data_variants, this](MutableColumns & key_columns, MutableColumns & final_aggregate_columns)
    { convertToBlockImplFinal(data_variants, key_columns, final_aggregate_columns); };

    return prepareBlockAndFill(data_variants, final, rows, filler);
}


}
