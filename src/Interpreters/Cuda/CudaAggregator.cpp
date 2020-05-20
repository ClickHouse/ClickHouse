#include <iomanip>
#include <thread>
#include <future>

#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>

#include <Interpreters/Cuda/CudaAggregator.h>
#include <Common/ClickHouseRevision.h>
#include <Common/MemoryTracker.h>
#include <Common/typeid_cast.h>
#include <Common/demangle.h>
#include <Interpreters/config_compile.h>


namespace ProfileEvents
{
    extern const Event ExternalAggregationWritePart;
    extern const Event ExternalAggregationCompressedBytes;
    extern const Event ExternalAggregationUncompressedBytes;
}

namespace CurrentMetrics
{
    extern const Metric QueryThread;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int EMPTY_DATA_PASSED;
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
                type = std::make_shared<DataTypeAggregateFunction>(params.aggregates[i].function, argument_types, params.aggregates[i].parameters);

            res.insert({ type, params.aggregates[i].column_name });
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


CudaAggregator::CudaAggregator(const Context & context_, const Params & params_)
    : context(context_), params(params_)
{
    /// Here we cut off unsupported cases

    if (params.keys_size != 1)
        throw Exception("CudaAggregator: params.keys_size is not equal 1", ErrorCodes::CUDA_UNSUPPORTED_CASE);
    if (params.aggregates_size != 1) 
        throw Exception("CudaAggregator: params.aggregates_size is not equal 1", ErrorCodes::CUDA_UNSUPPORTED_CASE);

    const auto & key_pos = params.keys[0];
    const auto & key_type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(key_pos).type;
    
    if (key_type->isNullable())
        throw Exception("CudaAggregator: have no idea what is nullable key", ErrorCodes::CUDA_UNSUPPORTED_CASE);
    if (!key_type->isString())
        throw Exception("CudaAggregator: key is not String", ErrorCodes::CUDA_UNSUPPORTED_CASE);

    /// Throws an exception if function CUDA version is not implemented
    cuda_agg_function = params.aggregates[0].function->createCudaFunction();

    if (params.aggregates[0].arguments.size() != 1)
        throw Exception("CudaAggregator: arguments number of function is not equal 1", ErrorCodes::CUDA_UNSUPPORTED_CASE);

    const auto & arg_pos = params.aggregates[0].arguments[0];
    const auto & arg_type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(arg_pos).type;

    if (arg_type->isNullable())
        throw Exception("CudaAggregator: have no idea what is nullable argument", ErrorCodes::CUDA_UNSUPPORTED_CASE);
    if (!arg_type->isString())
        throw Exception("CudaAggregator: argument is not String", ErrorCodes::CUDA_UNSUPPORTED_CASE);    
}


bool CudaAggregator::executeOnBlock(const Block & block, CudaAggregatedDataVariants & result,
    ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns)
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
        key_columns[i] = block.safeGetByPosition(params.keys[i]).column.get();

        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            materialized_columns.push_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }

    //AggregateFunctionInstructions aggregate_functions_instructions(params.aggregates_size + 1);
    //aggregate_functions_instructions[params.aggregates_size].that = nullptr;

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            aggregate_columns[i][j] = block.safeGetByPosition(params.aggregates[i].arguments[j]).column.get();

            if (ColumnPtr converted = aggregate_columns[i][j]->convertToFullColumnIfConst())
            {
                materialized_columns.push_back(converted);
                aggregate_columns[i][j] = materialized_columns.back().get();
            }
        }

        /*aggregate_functions_instructions[i].that = aggregate_functions[i];
        aggregate_functions_instructions[i].func = aggregate_functions[i]->getAddressOfAddFunction();
        aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];
        aggregate_functions_instructions[i].arguments = aggregate_columns[i].data();*/
    }

    size_t rows = block.rows();

    if (result.empty())
    {
        result.init(context, cuda_agg_function);
        result.startProcessing();
        //result.keys_size = params.keys_size;
        //result.key_sizes = key_sizes;
    }

    /// TODO get rid of this const_cast (problems is getChars and getOffsets been nonconst methods)
    ColumnString    *keys_column = const_cast<ColumnString*>(static_cast<const ColumnString*>(key_columns[0])),
                    *vals_column = const_cast<ColumnString*>(static_cast<const ColumnString*>(aggregate_columns[0][0]));

    const Settings & settings = context.getSettingsRef();
    result.strings_agg->queueData(rows, 
        keys_column->getChars().size(), reinterpret_cast<const char*>(keys_column->getChars().data()), keys_column->getOffsets().data(),
        vals_column->getChars().size(), reinterpret_cast<const char*>(vals_column->getChars().data()), vals_column->getOffsets().data());
    result.strings_agg->waitQueueData();

    return true;
}


void CudaAggregator::execute(const BlockInputStreamPtr & stream, CudaAggregatedDataVariants & result)
{
    //StringRefs key(params.keys_size);
    ColumnRawPtrs key_columns(params.keys_size);
    AggregateColumns aggregate_columns(params.aggregates_size);


    LOG_TRACE(log, "Cuda aggregating");

    Stopwatch watch;

    size_t src_rows = 0;
    size_t src_bytes = 0;

    /// Read all the data
    while (Block block = stream->read())
    {
        src_rows += block.rows();
        src_bytes += block.bytes();

        if (!executeOnBlock(block, result, key_columns, aggregate_columns))
            break;

    }

    result.waitProcessed();

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = result.strings_agg->getResult().size();
    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
        << " in " << elapsed_seconds << " sec."
        << " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
}


void NO_INLINE CudaAggregator::convertToBlockImplFinal(
    CudaAggregatedDataVariants & data_variants,
    MutableColumns & key_columns,
    MutableColumns & final_aggregate_columns) const
{
    for (const auto &elem : data_variants.strings_agg->getResult() ) 
    {
        key_columns[0]->insertData(elem.first.c_str(), elem.first.length());

        /// TODO we must have special interface for this insertion
        UInt64  res = cuda_agg_function->getResult(elem.second);
        static_cast<ColumnUInt64 &>(*final_aggregate_columns[0]).getData().push_back(res);
    }

    //destroyImpl<Method>(data);      /// NOTE You can do better.
}


template <typename Filler>
Block CudaAggregator::prepareBlockAndFill(
    CudaAggregatedDataVariants & data_variants,
    bool final,
    size_t rows,
    Filler && filler) const
{
    /// TODO unused parameter
    if (data_variants.empty()) {}

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

        /*if (params.aggregates[i].function->isState())
        {
            /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
            ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*final_aggregate_columns[i]);

            for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
                column_aggregate_func.addArena(data_variants.aggregates_pools[j]);
        }*/
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
        if (res.getByPosition(i).column->isColumnConst())
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}


Block CudaAggregator::prepareBlockAndFillSingleLevel(CudaAggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = data_variants.strings_agg->getResult().size();

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        MutableColumns & final_aggregate_columns)
    {
        convertToBlockImplFinal(data_variants, key_columns, final_aggregate_columns);
    };

    return prepareBlockAndFill(data_variants, final, rows, filler);
}


BlocksList CudaAggregator::convertToBlocks(CudaAggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    /// TODO unused parameters
    max_threads = max_threads;

    LOG_TRACE(log, "Converting aggregated data to blocks");

    Stopwatch watch;

    if (!final) {
        throw Exception("CudaAggregator::convertToBlocks: not final case is not supported yet ", 
            ErrorCodes::CUDA_UNSUPPORTED_CASE);
    }

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    blocks.emplace_back(prepareBlockAndFillSingleLevel(data_variants, final));

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Converted aggregated data to blocks. "
        << rows << " rows, " << bytes / 1048576.0 << " MiB"
        << " in " << elapsed_seconds << " sec."
        << " (" << rows / elapsed_seconds << " rows/sec., " << bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

    return blocks;
}


}
