#include <iomanip>
#include <thread>
#include <future>
#include <Poco/Util/Application.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <Interpreters/Aggregator.h>
#include <Common/ClickHouseRevision.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <common/demangle.h>
#if __has_include(<Interpreters/config_compile.h>)
#include <Interpreters/config_compile.h>
#endif


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
    extern const int CANNOT_COMPILE_CODE;
    extern const int TOO_MANY_ROWS;
    extern const int EMPTY_DATA_PASSED;
    extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
    extern const int LOGICAL_ERROR;
}


AggregatedDataVariants::~AggregatedDataVariants()
{
    if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
    {
        try
        {
            aggregator->destroyAllAggregateStates(*this);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


void AggregatedDataVariants::convertToTwoLevel()
{
    if (aggregator)
        LOG_TRACE(aggregator->log, "Converting aggregation data to two-level.");

    switch (type)
    {
    #define M(NAME) \
        case Type::NAME: \
            NAME ## _two_level = std::make_unique<decltype(NAME ## _two_level)::element_type>(*NAME); \
            NAME.reset(); \
            type = Type::NAME ## _two_level; \
            break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M

        default:
            throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
    }
}


Block Aggregator::getHeader(bool final) const
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


Aggregator::Aggregator(const Params & params_)
    : params(params_),
    isCancelled([]() { return false; })
{
    /// Use query-level memory tracker
    if (auto memory_tracker = CurrentThread::getMemoryTracker().getParent())
        memory_usage_before_aggregation = memory_tracker->get();

    aggregate_functions.resize(params.aggregates_size);
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i] = params.aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params.aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    // aggreate_states will be aligned as below:
    // |<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    //
    // pad_N will be used to match alignment requirement for each next state.
    // The address of state_1 is aligned based on maximum alignment requirements in states
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        offsets_of_aggregate_states[i] = total_size_of_aggregate_states;

        total_size_of_aggregate_states += params.aggregates[i].function->sizeOfData();

        // aggreate states are aligned based on maximum requirement
        align_aggregate_states = std::max(align_aggregate_states, params.aggregates[i].function->alignOfData());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < params.aggregates_size)
        {
            size_t alignment_of_next_state = params.aggregates[i + 1].function->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception("Logical error: alignOfData is not 2^N", ErrorCodes::LOGICAL_ERROR);

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            total_size_of_aggregate_states = (total_size_of_aggregate_states + alignment_of_next_state - 1) / alignment_of_next_state * alignment_of_next_state;
        }

        if (!params.aggregates[i].function->hasTrivialDestructor())
            all_aggregates_has_trivial_destructor = false;
    }

    method_chosen = chooseAggregationMethod();
}


void Aggregator::compileIfPossible(AggregatedDataVariants::Type type)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (compiled_if_possible)
        return;

    compiled_if_possible = true;

#if !defined(INTERNAL_COMPILER_HEADERS)
    throw Exception("Cannot compile code: Compiler disabled", ErrorCodes::CANNOT_COMPILE_CODE);
#else
    std::string method_typename_single_level;
    std::string method_typename_two_level;

    if (false) {}
#define M(NAME) \
    else if (type == AggregatedDataVariants::Type::NAME) \
    { \
        method_typename_single_level = "decltype(AggregatedDataVariants::" #NAME ")::element_type"; \
        method_typename_two_level = "decltype(AggregatedDataVariants::" #NAME "_two_level)::element_type"; \
    }

    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M

#define M(NAME) \
    else if (type == AggregatedDataVariants::Type::NAME) \
        method_typename_single_level = "decltype(AggregatedDataVariants::" #NAME ")::element_type";

    APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else if (type == AggregatedDataVariants::Type::without_key) {}
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    auto compiler_headers = Poco::Util::Application::instance().config().getString("compiler_headers", INTERNAL_COMPILER_HEADERS);

    /// List of types of aggregate functions.
    std::stringstream aggregate_functions_typenames_str;
    std::stringstream aggregate_functions_headers_args;
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        IAggregateFunction & func = *aggregate_functions[i];

        int status = 0;
        std::string type_name = demangle(typeid(func).name(), status);

        if (status)
            throw Exception("Cannot compile code: cannot demangle name " + String(typeid(func).name())
                + ", status: " + toString(status), ErrorCodes::CANNOT_COMPILE_CODE);

        aggregate_functions_typenames_str << ((i != 0) ? ", " : "") << type_name;

        std::string header_path = func.getHeaderFilePath();
        auto pos = header_path.find("/AggregateFunctions/");

        if (pos == std::string::npos)
            throw Exception("Cannot compile code: unusual path of header file for aggregate function: " + header_path,
                ErrorCodes::CANNOT_COMPILE_CODE);

        aggregate_functions_headers_args << "-include '" << compiler_headers << "/dbms/src";
        aggregate_functions_headers_args.write(&header_path[pos], header_path.size() - pos);
        aggregate_functions_headers_args << "' ";
    }

    aggregate_functions_headers_args << "-include '" << compiler_headers << "/dbms/src/Interpreters/SpecializedAggregator.h'";

    std::string aggregate_functions_typenames = aggregate_functions_typenames_str.str();

    std::stringstream key_str;
    key_str << "Aggregate: ";
    if (!method_typename_single_level.empty())
        key_str << method_typename_single_level + ", ";
    key_str << aggregate_functions_typenames;
    std::string key = key_str.str();

    auto get_code = [method_typename_single_level, method_typename_two_level, aggregate_functions_typenames]
    {
        /// A short piece of code, which is an explicit instantiation of the template.
        std::stringstream code;
        code <<     /// No explicit inclusion of the header file. It is included using the -include compiler option.
            "namespace DB\n"
            "{\n"
            "\n";

        /// There can be up to two instantiations for the template - for normal and two_level options.
        auto append_code_for_specialization =
            [&code, &aggregate_functions_typenames] (const std::string & method_typename, const std::string & suffix)
        {
            code <<
                "template void Aggregator::executeSpecialized<\n"
                    "    " << method_typename << ", TypeList<" << aggregate_functions_typenames << ">>(\n"
                    "    " << method_typename << " &, Arena *, size_t, ColumnRawPtrs &,\n"
                    "    AggregateColumns &, StringRefs &, bool, AggregateDataPtr) const;\n"
                "\n"
                "static void wrapper" << suffix << "(\n"
                    "    const Aggregator & aggregator,\n"
                    "    " << method_typename << " & method,\n"
                    "    Arena * arena,\n"
                    "    size_t rows,\n"
                    "    ColumnRawPtrs & key_columns,\n"
                    "    Aggregator::AggregateColumns & aggregate_columns,\n"
                    "    StringRefs & keys,\n"
                    "    bool no_more_keys,\n"
                    "    AggregateDataPtr overflow_row)\n"
                "{\n"
                    "    aggregator.executeSpecialized<\n"
                        "        " << method_typename << ", TypeList<" << aggregate_functions_typenames << ">>(\n"
                        "        method, arena, rows, key_columns, aggregate_columns, keys, no_more_keys, overflow_row);\n"
                "}\n"
                "\n"
                "void * getPtr" << suffix << "() __attribute__((__visibility__(\"default\")));\n"
                "void * getPtr" << suffix << "()\n" /// Without this wrapper, it's not clear how to get the desired symbol from the compiled library.
                "{\n"
                    "    return reinterpret_cast<void *>(&wrapper" << suffix << ");\n"
                "}\n";
        };

        if (!method_typename_single_level.empty())
            append_code_for_specialization(method_typename_single_level, "");
        else
        {
            /// For `without_key` method.
            code <<
                "template void Aggregator::executeSpecializedWithoutKey<\n"
                    "    " << "TypeList<" << aggregate_functions_typenames << ">>(\n"
                    "    AggregatedDataWithoutKey &, size_t, AggregateColumns &, Arena *) const;\n"
                "\n"
                "static void wrapper(\n"
                    "    const Aggregator & aggregator,\n"
                    "    AggregatedDataWithoutKey & method,\n"
                    "    size_t rows,\n"
                    "    Aggregator::AggregateColumns & aggregate_columns,\n"
                    "    Arena * arena)\n"
                "{\n"
                    "    aggregator.executeSpecializedWithoutKey<\n"
                        "        TypeList<" << aggregate_functions_typenames << ">>(\n"
                        "        method, rows, aggregate_columns, arena);\n"
                "}\n"
                "\n"
                "void * getPtr() __attribute__((__visibility__(\"default\")));\n"
                "void * getPtr()\n"
                "{\n"
                    "    return reinterpret_cast<void *>(&wrapper);\n"
                "}\n";
        }

        if (!method_typename_two_level.empty())
            append_code_for_specialization(method_typename_two_level, "TwoLevel");
        else
        {
            /// The stub.
            code <<
                "void * getPtrTwoLevel() __attribute__((__visibility__(\"default\")));\n"
                "void * getPtrTwoLevel()\n"
                "{\n"
                    "    return nullptr;\n"
                "}\n";
        }

        code <<
            "}\n";

        return code.str();
    };

    auto compiled_data_owned_by_callback = compiled_data;
    auto on_ready = [compiled_data_owned_by_callback] (SharedLibraryPtr & lib)
    {
        if (compiled_data_owned_by_callback.unique())   /// Aggregator is already destroyed.
            return;

        compiled_data_owned_by_callback->compiled_aggregator = lib;
        compiled_data_owned_by_callback->compiled_method_ptr = lib->get<void * (*) ()>("_ZN2DB6getPtrEv")();
        compiled_data_owned_by_callback->compiled_two_level_method_ptr = lib->get<void * (*) ()>("_ZN2DB14getPtrTwoLevelEv")();
    };

    /** If the library has already been compiled, a non-zero SharedLibraryPtr is returned.
      * If the library was not compiled, then the counter is incremented, and nullptr is returned.
      * If the counter has reached the value min_count_to_compile, then the compilation starts asynchronously (in a separate thread)
      *  at the end of which `on_ready` callback is called.
      */
    aggregate_functions_headers_args << " -Wno-unused-function";
    SharedLibraryPtr lib = params.compiler->getOrCount(key, params.min_count_to_compile,
        aggregate_functions_headers_args.str(),
        get_code, on_ready);

    /// If the result is already ready.
    if (lib)
        on_ready(lib);
#endif
}


AggregatedDataVariants::Type Aggregator::chooseAggregationMethod()
{
    /// If no keys. All aggregating to single row.
    if (params.keys_size == 0)
        return AggregatedDataVariants::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(params.keys.size());
    bool has_nullable_key = false;

    for (const auto & pos : params.keys)
    {
        const auto & type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(pos).type;

        if (type->isNullable())
        {
            has_nullable_key = true;
            types_removed_nullable.push_back(removeNullable(type));
        }
        else
            types_removed_nullable.push_back(type);
    }

    /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    key_sizes.resize(params.keys_size);
    for (size_t j = 0; j < params.keys_size; ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                ++num_fixed_contiguous_keys;
                key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += key_sizes[j];
            }
        }
    }

    if (has_nullable_key)
    {
        if (params.keys_size == num_fixed_contiguous_keys)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return AggregatedDataVariants::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return AggregatedDataVariants::Type::nullable_keys256;
        }

        /// Fallback case.
        return AggregatedDataVariants::Type::serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params.keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();
        if (size_of_field == 1)
            return AggregatedDataVariants::Type::key8;
        if (size_of_field == 2)
            return AggregatedDataVariants::Type::key16;
        if (size_of_field == 4)
            return AggregatedDataVariants::Type::key32;
        if (size_of_field == 8)
            return AggregatedDataVariants::Type::key64;
        if (size_of_field == 16)
            return AggregatedDataVariants::Type::keys128;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params.keys_size == num_fixed_contiguous_keys)
    {
        if (keys_bytes <= 16)
            return AggregatedDataVariants::Type::keys128;
        if (keys_bytes <= 32)
            return AggregatedDataVariants::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (params.keys_size == 1 && types_removed_nullable[0]->isString())
        return AggregatedDataVariants::Type::key_string;

    if (params.keys_size == 1 && types_removed_nullable[0]->isFixedString())
        return AggregatedDataVariants::Type::key_fixed_string;

    return AggregatedDataVariants::Type::serialized;
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        try
        {
            /** An exception may occur if there is a shortage of memory.
              * In order that then everything is properly destroyed, we "roll back" some of the created states.
              * The code is not very convenient.
              */
            aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
        }
        catch (...)
        {
            for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
                aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

            throw;
        }
    }
}


/** It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
  * (Probably because after the inline of this function, more internal functions no longer be inlined.)
  * Inline does not make sense, since the inner loop is entirely inside this function.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
    Method & method,
    Arena * aggregates_pool,
    size_t rows,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    StringRefs & keys,
    bool no_more_keys,
    AggregateDataPtr overflow_row) const
{
    typename Method::State state;
    state.init(key_columns);

    if (!no_more_keys)
        executeImplCase<false>(method, state, aggregates_pool, rows, key_columns, aggregate_instructions, keys, overflow_row);
    else
        executeImplCase<true>(method, state, aggregates_pool, rows, key_columns, aggregate_instructions, keys, overflow_row);
}

#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

template <bool no_more_keys, typename Method>
void NO_INLINE Aggregator::executeImplCase(
    Method & method,
    typename Method::State & state,
    Arena * aggregates_pool,
    size_t rows,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    StringRefs & keys,
    AggregateDataPtr overflow_row) const
{
    /// NOTE When editing this code, also pay attention to SpecializedAggregator.h.

    /// For all rows.
    typename Method::iterator it;
    typename Method::Key prev_key;
    for (size_t i = 0; i < rows; ++i)
    {
        bool inserted;          /// Inserted a new key, or was this key already?
        bool overflow = false;  /// The new key did not fit in the hash table because of no_more_keys.

        /// Get the key to insert into the hash table.
        typename Method::Key key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *aggregates_pool);

        if (!no_more_keys)  /// Insert.
        {
            /// Optimization for consecutive identical keys.
            if (!Method::no_consecutive_keys_optimization)
            {
                if (i != 0 && key == prev_key)
                {
                    /// Add values to the aggregate functions.
                    AggregateDataPtr value = Method::getAggregateData(it->second);
                    for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
                        (*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i, aggregates_pool);

                    method.onExistingKey(key, keys, *aggregates_pool);
                    continue;
                }
                else
                    prev_key = key;
            }

            method.data.emplace(key, it, inserted);
        }
        else
        {
            /// Add only if the key already exists.
            inserted = false;
            it = method.data.find(key);
            if (method.data.end() == it)
                overflow = true;
        }

        /// If the key does not fit, and the data does not need to be aggregated in a separate row, then there's nothing to do.
        if (no_more_keys && overflow && !overflow_row)
        {
            method.onExistingKey(key, keys, *aggregates_pool);
            continue;
        }

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (inserted)
        {
            AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);

            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            aggregate_data = nullptr;

            method.onNewKey(*it, params.keys_size, keys, *aggregates_pool);

            AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(place);
            aggregate_data = place;
        }
        else
            method.onExistingKey(key, keys, *aggregates_pool);

        AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

        /// Add values to the aggregate functions.
        for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
            (*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i, aggregates_pool);
    }
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

void NO_INLINE Aggregator::executeWithoutKeyImpl(
    AggregatedDataWithoutKey & res,
    size_t rows,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena) const
{
    /// Optimization in the case of a single aggregate function `count`.
    AggregateFunctionCount * agg_count = params.aggregates_size == 1
        ? typeid_cast<AggregateFunctionCount *>(aggregate_functions[0])
        : nullptr;

    if (agg_count)
        agg_count->addDelta(res, rows);
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            /// Adding values
            for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
                (*inst->func)(inst->that, res + inst->state_offset, inst->arguments, i, arena);
        }
    }
}


bool Aggregator::executeOnBlock(const Block & block, AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns, StringRefs & key,
    bool & no_more_keys)
{
    if (isCancelled())
        return true;

    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

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

    AggregateFunctionInstructions aggregate_functions_instructions(params.aggregates_size + 1);
    aggregate_functions_instructions[params.aggregates_size].that = nullptr;

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

        aggregate_functions_instructions[i].that = aggregate_functions[i];
        aggregate_functions_instructions[i].func = aggregate_functions[i]->getAddressOfAddFunction();
        aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];
        aggregate_functions_instructions[i].arguments = aggregate_columns[i].data();
    }

    if (isCancelled())
        return true;

    size_t rows = block.rows();

    /// How to perform the aggregation?
    if (result.empty())
    {
        result.init(method_chosen);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
        LOG_TRACE(log, "Aggregation method: " << result.getMethodName());

        if (params.compiler)
            compileIfPossible(result.type);
    }

    if (isCancelled())
        return true;

    if ((params.overflow_row || result.type == AggregatedDataVariants::Type::without_key) && !result.without_key)
    {
        AggregateDataPtr place = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        result.without_key = place;
    }

    /// We select one of the aggregation methods and call it.

    /// For the case when there are no keys (all aggregate into one row).
    if (result.type == AggregatedDataVariants::Type::without_key)
    {
        /// If there is a dynamically compiled code.
        if (compiled_data->compiled_method_ptr)
        {
            reinterpret_cast<
                void (*)(const Aggregator &, AggregatedDataWithoutKey &, size_t, AggregateColumns &, Arena *)>
                    (compiled_data->compiled_method_ptr)(*this, result.without_key, rows, aggregate_columns, result.aggregates_pool);
        }
        else
            executeWithoutKeyImpl(result.without_key, rows, aggregate_functions_instructions.data(), result.aggregates_pool);
    }
    else
    {
        /// This is where data is written that does not fit in `max_rows_to_group_by` with `group_by_overflow_mode = any`.
        AggregateDataPtr overflow_row_ptr = params.overflow_row ? result.without_key : nullptr;

        bool is_two_level = result.isTwoLevel();

        /// Compiled code, for the normal structure.
        if (!is_two_level && compiled_data->compiled_method_ptr)
        {
        #define M(NAME, IS_TWO_LEVEL) \
            else if (result.type == AggregatedDataVariants::Type::NAME) \
                reinterpret_cast<void (*)( \
                    const Aggregator &, decltype(result.NAME)::element_type &, \
                    Arena *, size_t, ColumnRawPtrs &, AggregateColumns &, \
                    StringRefs &, bool, AggregateDataPtr)>(compiled_data->compiled_method_ptr) \
                (*this, *result.NAME, result.aggregates_pool, rows, key_columns, aggregate_columns, \
                    key, no_more_keys, overflow_row_ptr);

            if (false) {}
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
        }
        /// Compiled code, for a two-level structure.
        else if (is_two_level && compiled_data->compiled_two_level_method_ptr)
        {
        #define M(NAME) \
            else if (result.type == AggregatedDataVariants::Type::NAME) \
                reinterpret_cast<void (*)( \
                    const Aggregator &, decltype(result.NAME)::element_type &, \
                    Arena *, size_t, ColumnRawPtrs &, AggregateColumns &, \
                    StringRefs &, bool, AggregateDataPtr)>(compiled_data->compiled_two_level_method_ptr) \
                (*this, *result.NAME, result.aggregates_pool, rows, key_columns, aggregate_columns, \
                    key, no_more_keys, overflow_row_ptr);

            if (false) {}
            APPLY_FOR_VARIANTS_TWO_LEVEL(M)
        #undef M
        }
        /// When there is no dynamically compiled code.
        else
        {
        #define M(NAME, IS_TWO_LEVEL) \
            else if (result.type == AggregatedDataVariants::Type::NAME) \
                executeImpl(*result.NAME, result.aggregates_pool, rows, key_columns, aggregate_functions_instructions.data(), \
                    key, no_more_keys, overflow_row_ptr);

            if (false) {}
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
        }
    }

    size_t result_size = result.sizeWithoutOverflowRow();
    Int64 current_memory_usage = 0;
    if (auto memory_tracker = CurrentThread::getMemoryTracker().getParent())
        current_memory_usage = memory_tracker->get();

    auto result_size_bytes = current_memory_usage - memory_usage_before_aggregation;    /// Here all the results in the sum are taken into account, from different threads.

    bool worth_convert_to_two_level
        = (params.group_by_two_level_threshold && result_size >= params.group_by_two_level_threshold)
        || (params.group_by_two_level_threshold_bytes && result_size_bytes >= static_cast<Int64>(params.group_by_two_level_threshold_bytes));

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
        result.convertToTwoLevel();

    /// Checking the constraints.
    if (!checkLimits(result_size, no_more_keys))
        return false;

    /** Flush data to disk if too much RAM is consumed.
      * Data can only be flushed to disk if a two-level aggregation structure is used.
      */
    if (params.max_bytes_before_external_group_by
        && result.isTwoLevel()
        && current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by)
        && worth_convert_to_two_level)
    {
        writeToTemporaryFile(result);
    }

    return true;
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants)
{
    Stopwatch watch;
    size_t rows = data_variants.size();

    Poco::File(params.tmp_path).createDirectories();
    auto file = std::make_unique<Poco::TemporaryFile>(params.tmp_path);
    const std::string & path = file->path();
    WriteBufferFromFile file_buf(path);
    CompressedWriteBuffer compressed_buf(file_buf);
    NativeBlockOutputStream block_out(compressed_buf, ClickHouseRevision::get(), getHeader(false));

    LOG_DEBUG(log, "Writing part of aggregation data into temporary file " << path << ".");
    ProfileEvents::increment(ProfileEvents::ExternalAggregationWritePart);

    /// Flush only two-level data and possibly overflow data.

#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        writeToTemporaryFileImpl(data_variants, *data_variants.NAME, block_out);

    if (false) {}
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    /// NOTE Instead of freeing up memory and creating new hash tables and arenas, you can re-use the old ones.
    data_variants.init(data_variants.type);
    data_variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();
    data_variants.without_key = nullptr;

    block_out.flush();
    compressed_buf.next();
    file_buf.next();

    double elapsed_seconds = watch.elapsedSeconds();
    double compressed_bytes = file_buf.count();
    double uncompressed_bytes = compressed_buf.count();

    {
        std::lock_guard<std::mutex> lock(temporary_files.mutex);
        temporary_files.files.emplace_back(std::move(file));
        temporary_files.sum_size_uncompressed += uncompressed_bytes;
        temporary_files.sum_size_compressed += compressed_bytes;
    }

    ProfileEvents::increment(ProfileEvents::ExternalAggregationCompressedBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationUncompressedBytes, uncompressed_bytes);

    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Written part in " << elapsed_seconds << " sec., "
        << rows << " rows, "
        << (uncompressed_bytes / 1048576.0) << " MiB uncompressed, "
        << (compressed_bytes / 1048576.0) << " MiB compressed, "
        << (uncompressed_bytes / rows) << " uncompressed bytes per row, "
        << (compressed_bytes / rows) << " compressed bytes per row, "
        << "compression rate: " << (uncompressed_bytes / compressed_bytes)
        << " (" << (rows / elapsed_seconds) << " rows/sec., "
        << (uncompressed_bytes / elapsed_seconds / 1048576.0) << " MiB/sec. uncompressed, "
        << (compressed_bytes / elapsed_seconds / 1048576.0) << " MiB/sec. compressed)");
}


template <typename Method>
Block Aggregator::convertOneBucketToBlock(
    AggregatedDataVariants & data_variants,
    Method & method,
    bool final,
    size_t bucket) const
{
    Block block = prepareBlockAndFill(data_variants, final, method.data.impls[bucket].size(),
        [bucket, &method, this] (
            MutableColumns & key_columns,
            AggregateColumnsData & aggregate_columns,
            MutableColumns & final_aggregate_columns,
            bool final_)
        {
            convertToBlockImpl(method, method.data.impls[bucket],
                key_columns, aggregate_columns, final_aggregate_columns, final_);
        });

    block.info.bucket_num = bucket;
    return block;
}


template <typename Method>
void Aggregator::writeToTemporaryFileImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    IBlockOutputStream & out)
{
    size_t max_temporary_block_size_rows = 0;
    size_t max_temporary_block_size_bytes = 0;

    auto update_max_sizes = [&](const Block & block)
    {
        size_t block_size_rows = block.rows();
        size_t block_size_bytes = block.bytes();

        if (block_size_rows > max_temporary_block_size_rows)
            max_temporary_block_size_rows = block_size_rows;
        if (block_size_bytes > max_temporary_block_size_bytes)
            max_temporary_block_size_bytes = block_size_bytes;
    };

    for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
    {
        Block block = convertOneBucketToBlock(data_variants, method, false, bucket);
        out.write(block);
        update_max_sizes(block);
    }

    if (params.overflow_row)
    {
        Block block = prepareBlockAndFillWithoutKey(data_variants, false, true);
        out.write(block);
        update_max_sizes(block);
    }

    /// Pass ownership of the aggregate functions states:
    /// `data_variants` will not destroy them in the destructor, they are now owned by ColumnAggregateFunction objects.
    data_variants.aggregator = nullptr;

    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Max size of temporary block: " << max_temporary_block_size_rows << " rows, "
        << (max_temporary_block_size_bytes / 1048576.0) << " MiB.");
}


bool Aggregator::checkLimits(size_t result_size, bool & no_more_keys) const
{
    if (!no_more_keys && params.max_rows_to_group_by && result_size > params.max_rows_to_group_by)
    {
        switch (params.group_by_overflow_mode)
        {
            case OverflowMode::THROW:
                throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
                    + " rows, maximum: " + toString(params.max_rows_to_group_by),
                    ErrorCodes::TOO_MANY_ROWS);

            case OverflowMode::BREAK:
                return false;

            case OverflowMode::ANY:
                no_more_keys = true;
                break;
        }
    }

    return true;
}


void Aggregator::execute(const BlockInputStreamPtr & stream, AggregatedDataVariants & result)
{
    if (isCancelled())
        return;

    StringRefs key(params.keys_size);
    ColumnRawPtrs key_columns(params.keys_size);
    AggregateColumns aggregate_columns(params.aggregates_size);

    /** Used if there is a limit on the maximum number of rows in the aggregation,
      *  and if group_by_overflow_mode == ANY.
      * In this case, new keys are not added to the set, but aggregation is performed only by
      *  keys that have already managed to get into the set.
      */
    bool no_more_keys = false;

    LOG_TRACE(log, "Aggregating");

    Stopwatch watch;

    size_t src_rows = 0;
    size_t src_bytes = 0;

    /// Read all the data
    while (Block block = stream->read())
    {
        if (isCancelled())
            return;

        src_rows += block.rows();
        src_bytes += block.bytes();

        if (!executeOnBlock(block, result, key_columns, aggregate_columns, key, no_more_keys))
            break;
    }

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (result.empty() && params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
        executeOnBlock(stream->getHeader(), result, key_columns, aggregate_columns, key, no_more_keys);

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = result.sizeWithoutOverflowRow();
    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
        << " in " << elapsed_seconds << " sec."
        << " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
}


template <typename Method, typename Table>
void Aggregator::convertToBlockImpl(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    AggregateColumnsData & aggregate_columns,
    MutableColumns & final_aggregate_columns,
    bool final) const
{
    if (data.empty())
        return;

    if (final)
        convertToBlockImplFinal(method, data, key_columns, final_aggregate_columns);
    else
        convertToBlockImplNotFinal(method, data, key_columns, aggregate_columns);

    /// In order to release memory early.
    data.clearAndShrink();
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplFinal(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    MutableColumns & final_aggregate_columns) const
{
    for (const auto & value : data)
    {
        method.insertKeyIntoColumns(value, key_columns, params.keys_size, key_sizes);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->insertResultInto(
                Method::getAggregateData(value.second) + offsets_of_aggregate_states[i],
                *final_aggregate_columns[i]);
    }

    destroyImpl<Method>(data);      /// NOTE You can do better.
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    AggregateColumnsData & aggregate_columns) const
{

    for (auto & value : data)
    {
        method.insertKeyIntoColumns(value, key_columns, params.keys_size, key_sizes);

        /// reserved, so push_back does not throw exceptions
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_columns[i]->push_back(Method::getAggregateData(value.second) + offsets_of_aggregate_states[i]);

        Method::getAggregateData(value.second) = nullptr;
    }
}


template <typename Filler>
Block Aggregator::prepareBlockAndFill(
    AggregatedDataVariants & data_variants,
    bool final,
    size_t rows,
    Filler && filler) const
{
    MutableColumns key_columns(params.keys_size);
    MutableColumns aggregate_columns(params.aggregates_size);
    MutableColumns final_aggregate_columns(params.aggregates_size);
    AggregateColumnsData aggregate_columns_data(params.aggregates_size);

    Block header = getHeader(final);

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        key_columns[i] = header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (!final)
        {
            aggregate_columns[i] = header.safeGetByPosition(i + params.keys_size).type->createColumn();

            /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
            ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

            for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
                column_aggregate_func.addArena(data_variants.aggregates_pools[j]);

            aggregate_columns_data[i] = &column_aggregate_func.getData();
            aggregate_columns_data[i]->reserve(rows);
        }
        else
        {
            final_aggregate_columns[i] = aggregate_functions[i]->getReturnType()->createColumn();
            final_aggregate_columns[i]->reserve(rows);

            if (aggregate_functions[i]->isState())
            {
                /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                ColumnAggregateFunction & column_aggregate_func = static_cast<ColumnAggregateFunction &>(*final_aggregate_columns[i]);

                for (size_t j = 0; j < data_variants.aggregates_pools.size(); ++j)
                    column_aggregate_func.addArena(data_variants.aggregates_pools[j]);
            }
        }
    }

    filler(key_columns, aggregate_columns_data, final_aggregate_columns, final);

    Block res = header.cloneEmpty();

    for (size_t i = 0; i < params.keys_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (final)
            res.getByPosition(i + params.keys_size).column = std::move(final_aggregate_columns[i]);
        else
            res.getByPosition(i + params.keys_size).column = std::move(aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = header.columns();
    for (size_t i = 0; i < columns; ++i)
        if (res.getByPosition(i).column->isColumnConst())
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}


Block Aggregator::prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const
{
    size_t rows = 1;

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        bool final_)
    {
        if (data_variants.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
        {
            AggregatedDataWithoutKey & data = data_variants.without_key;

            for (size_t i = 0; i < params.aggregates_size; ++i)
            {
                if (!final_)
                    aggregate_columns[i]->push_back(data + offsets_of_aggregate_states[i]);
                else
                    aggregate_functions[i]->insertResultInto(data + offsets_of_aggregate_states[i], *final_aggregate_columns[i]);
            }

            if (!final_)
                data = nullptr;

            if (params.overflow_row)
                for (size_t i = 0; i < params.keys_size; ++i)
                    key_columns[i]->insertDefault();
        }
    };

    Block block = prepareBlockAndFill(data_variants, final, rows, filler);

    if (is_overflows)
        block.info.is_overflows = true;

    if (final)
        destroyWithoutKey(data_variants);

    return block;
}

Block Aggregator::prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = data_variants.sizeWithoutOverflowRow();

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        bool final_)
    {
    #define M(NAME) \
        else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
            convertToBlockImpl(*data_variants.NAME, data_variants.NAME->data, \
                key_columns, aggregate_columns, final_aggregate_columns, final_);

        if (false) {}
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
    #undef M
        else
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    };

    return prepareBlockAndFill(data_variants, final, rows, filler);
}


BlocksList Aggregator::prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, ThreadPool * thread_pool) const
{
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, final, thread_pool);

    if (false) {}
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


template <typename Method>
BlocksList Aggregator::prepareBlocksAndFillTwoLevelImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    bool final,
    ThreadPool * thread_pool) const
{
    auto converter = [&](size_t bucket, ThreadGroupStatusPtr thread_group)
    {
        CurrentThread::attachToIfDetached(thread_group);
        return convertOneBucketToBlock(data_variants, method, final, bucket);
    };

    /// packaged_task is used to ensure that exceptions are automatically thrown into the main stream.

    std::vector<std::packaged_task<Block()>> tasks(Method::Data::NUM_BUCKETS);

    try
    {
        for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
        {
            if (method.data.impls[bucket].empty())
                continue;

            tasks[bucket] = std::packaged_task<Block()>(std::bind(converter, bucket, CurrentThread::getGroup()));

            if (thread_pool)
                thread_pool->schedule([bucket, &tasks] { tasks[bucket](); });
            else
                tasks[bucket]();
        }
    }
    catch (...)
    {
        /// If this is not done, then in case of an exception, tasks will be destroyed before the threads are completed, and it will be bad.
        if (thread_pool)
            thread_pool->wait();

        throw;
    }

    if (thread_pool)
        thread_pool->wait();

    BlocksList blocks;

    for (auto & task : tasks)
    {
        if (!task.valid())
            continue;

        blocks.emplace_back(task.get_future().get());
    }

    return blocks;
}


BlocksList Aggregator::convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    if (isCancelled())
        return BlocksList();

    LOG_TRACE(log, "Converting aggregated data to blocks");

    Stopwatch watch;

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000  /// TODO Make a custom threshold.
        && data_variants.isTwoLevel())                      /// TODO Use the shared thread pool with the `merge` function.
        thread_pool = std::make_unique<ThreadPool>(max_threads);

    if (isCancelled())
        return BlocksList();

    if (data_variants.without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(
            data_variants, final, data_variants.type != AggregatedDataVariants::Type::without_key));

    if (isCancelled())
        return BlocksList();

    if (data_variants.type != AggregatedDataVariants::Type::without_key)
    {
        if (!data_variants.isTwoLevel())
            blocks.emplace_back(prepareBlockAndFillSingleLevel(data_variants, final));
        else
            blocks.splice(blocks.end(), prepareBlocksAndFillTwoLevel(data_variants, final, thread_pool.get()));
    }

    if (!final)
    {
        /// data_variants will not destroy the states of aggregate functions in the destructor.
        /// Now ColumnAggregateFunction owns the states.
        data_variants.aggregator = nullptr;
    }

    if (isCancelled())
        return BlocksList();

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


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    for (auto it = table_src.begin(); it != table_src.end(); ++it)
    {
        decltype(it) res_it;
        bool inserted;
        table_dst.emplace(it->first, res_it, inserted, it.getHash());

        if (!inserted)
        {
            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_functions[i]->merge(
                    Method::getAggregateData(res_it->second) + offsets_of_aggregate_states[i],
                    Method::getAggregateData(it->second) + offsets_of_aggregate_states[i],
                    arena);

            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_functions[i]->destroy(
                    Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);
        }
        else
        {
            res_it->second = it->second;
        }

        Method::getAggregateData(it->second) = nullptr;
    }

    table_src.clearAndShrink();
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNoMoreKeysImpl(
    Table & table_dst,
    AggregatedDataWithoutKey & overflows,
    Table & table_src,
    Arena * arena) const
{
    for (auto it = table_src.begin(); it != table_src.end(); ++it)
    {
        decltype(it) res_it = table_dst.find(it->first, it.getHash());

        AggregateDataPtr res_data = table_dst.end() == res_it
            ? overflows
            : Method::getAggregateData(res_it->second);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                res_data + offsets_of_aggregate_states[i],
                Method::getAggregateData(it->second) + offsets_of_aggregate_states[i],
                arena);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(
                Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

        Method::getAggregateData(it->second) = nullptr;
    }

    table_src.clearAndShrink();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataOnlyExistingKeysImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    for (auto it = table_src.begin(); it != table_src.end(); ++it)
    {
        decltype(it) res_it = table_dst.find(it->first, it.getHash());

        if (table_dst.end() == res_it)
            continue;

        AggregateDataPtr res_data = Method::getAggregateData(res_it->second);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                res_data + offsets_of_aggregate_states[i],
                Method::getAggregateData(it->second) + offsets_of_aggregate_states[i],
                arena);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(
                Method::getAggregateData(it->second) + offsets_of_aggregate_states[i]);

        Method::getAggregateData(it->second) = nullptr;
    }

    table_src.clearAndShrink();
}


void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(
    ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        AggregatedDataWithoutKey & res_data = res->without_key;
        AggregatedDataWithoutKey & current_data = non_empty_data[result_num]->without_key;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i], res->aggregates_pool);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

        current_data = nullptr;
    }
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(
    ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];
    bool no_more_keys = false;

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        if (!checkLimits(res->sizeWithoutOverflowRow(), no_more_keys))
            break;

        AggregatedDataVariants & current = *non_empty_data[result_num];

        if (!no_more_keys)
            mergeDataImpl<Method>(
                getDataVariant<Method>(*res).data,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);
        else if (res->without_key)
            mergeDataNoMoreKeysImpl<Method>(
                getDataVariant<Method>(*res).data,
                res->without_key,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);
        else
            mergeDataOnlyExistingKeysImpl<Method>(
                getDataVariant<Method>(*res).data,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);

        /// `current` will not destroy the states of aggregate functions in the destructor
        current.aggregator = nullptr;
    }
}


template <typename Method>
void NO_INLINE Aggregator::mergeBucketImpl(
    ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena) const
{
    /// We merge all aggregation results to the first.
    AggregatedDataVariantsPtr & res = data[0];
    for (size_t result_num = 1, size = data.size(); result_num < size; ++result_num)
    {
        AggregatedDataVariants & current = *data[result_num];

        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data.impls[bucket],
            getDataVariant<Method>(current).data.impls[bucket],
            arena);
    }
}


/** Combines aggregation states together, turns them into blocks, and outputs streams.
  * If the aggregation states are two-level, then it produces blocks strictly in order of 'bucket_num'.
  * (This is important for distributed processing.)
  * In doing so, it can handle different buckets in parallel, using up to `threads` threads.
  */
class MergingAndConvertingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** The input is a set of non-empty sets of partially aggregated data,
      *  which are all either single-level, or are two-level.
      */
    MergingAndConvertingBlockInputStream(const Aggregator & aggregator_, ManyAggregatedDataVariants & data_, bool final_, size_t threads_)
        : aggregator(aggregator_), data(data_), final(final_), threads(threads_)
    {
        /// At least we need one arena in first data item per thread
        if (!data.empty() && threads > data[0]->aggregates_pools.size())
        {
            Arenas & first_pool = data[0]->aggregates_pools;
            for (size_t j = first_pool.size(); j < threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }
    }

    String getName() const override { return "MergingAndConverting"; }

    Block getHeader() const override { return aggregator.getHeader(final); }

    ~MergingAndConvertingBlockInputStream() override
    {
        LOG_TRACE(&Logger::get(__PRETTY_FUNCTION__), "Waiting for threads to finish");

        /// We need to wait for threads to finish before destructor of 'parallel_merge_data',
        ///  because the threads access 'parallel_merge_data'.
        if (parallel_merge_data)
            parallel_merge_data->pool.wait();
    }

protected:
    Block readImpl() override
    {
        if (data.empty())
            return {};

        if (current_bucket_num >= NUM_BUCKETS)
            return {};

        AggregatedDataVariantsPtr & first = data[0];

        if (current_bucket_num == -1)
        {
            ++current_bucket_num;

            if (first->type == AggregatedDataVariants::Type::without_key || aggregator.params.overflow_row)
            {
                aggregator.mergeWithoutKeyDataImpl(data);
                return aggregator.prepareBlockAndFillWithoutKey(
                    *first, final, first->type != AggregatedDataVariants::Type::without_key);
            }
        }

        if (!first->isTwoLevel())
        {
            if (current_bucket_num > 0)
                return {};

            if (first->type == AggregatedDataVariants::Type::without_key)
                return {};

            ++current_bucket_num;

        #define M(NAME) \
            else if (first->type == AggregatedDataVariants::Type::NAME) \
                aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(data);
            if (false) {}
            APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
        #undef M
            else
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

            return aggregator.prepareBlockAndFillSingleLevel(*first, final);
        }
        else
        {
            if (!parallel_merge_data)
            {
                parallel_merge_data = std::make_unique<ParallelMergeData>(threads);
                for (size_t i = 0; i < threads; ++i)
                    scheduleThreadForNextBucket();
            }

            Block res;

            while (true)
            {
                std::unique_lock<std::mutex> lock(parallel_merge_data->mutex);

                if (parallel_merge_data->exception)
                    std::rethrow_exception(parallel_merge_data->exception);

                auto it = parallel_merge_data->ready_blocks.find(current_bucket_num);
                if (it != parallel_merge_data->ready_blocks.end())
                {
                    ++current_bucket_num;
                    scheduleThreadForNextBucket();

                    if (it->second)
                    {
                        res.swap(it->second);
                        break;
                    }
                    else if (current_bucket_num >= NUM_BUCKETS)
                        break;
                }

                parallel_merge_data->condvar.wait(lock);
            }

            return res;
        }
    }

private:
    const Aggregator & aggregator;
    ManyAggregatedDataVariants data;
    bool final;
    size_t threads;

    Int32 current_bucket_num = -1;
    Int32 max_scheduled_bucket_num = -1;
    static constexpr Int32 NUM_BUCKETS = 256;

    struct ParallelMergeData
    {
        std::map<Int32, Block> ready_blocks;
        std::exception_ptr exception;
        std::mutex mutex;
        std::condition_variable condvar;
        ThreadPool pool;

        explicit ParallelMergeData(size_t threads) : pool(threads) {}
    };

    std::unique_ptr<ParallelMergeData> parallel_merge_data;

    void scheduleThreadForNextBucket()
    {
        ++max_scheduled_bucket_num;
        if (max_scheduled_bucket_num >= NUM_BUCKETS)
            return;

        parallel_merge_data->pool.schedule(std::bind(&MergingAndConvertingBlockInputStream::thread, this,
            max_scheduled_bucket_num, CurrentThread::getGroup()));
    }

    void thread(Int32 bucket_num, ThreadGroupStatusPtr thread_group)
    {
        try
        {
            setThreadName("MergingAggregtd");
            CurrentThread::attachToIfDetached(thread_group);
            CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

            /// TODO: add no_more_keys support maybe

            auto & merged_data = *data[0];
            auto method = merged_data.type;
            Block block;

            /// Select Arena to avoid race conditions
            size_t thread_number = static_cast<size_t>(bucket_num) % threads;
            Arena * arena = merged_data.aggregates_pools.at(thread_number).get();

            if (false) {}
        #define M(NAME) \
            else if (method == AggregatedDataVariants::Type::NAME) \
            { \
                aggregator.mergeBucketImpl<decltype(merged_data.NAME)::element_type>(data, bucket_num, arena); \
                block = aggregator.convertOneBucketToBlock(merged_data, *merged_data.NAME, final, bucket_num); \
            }

            APPLY_FOR_VARIANTS_TWO_LEVEL(M)
        #undef M

            std::lock_guard<std::mutex> lock(parallel_merge_data->mutex);
            parallel_merge_data->ready_blocks[bucket_num] = std::move(block);
        }
        catch (...)
        {
            std::lock_guard<std::mutex> lock(parallel_merge_data->mutex);
            if (!parallel_merge_data->exception)
                parallel_merge_data->exception = std::current_exception();
        }

        parallel_merge_data->condvar.notify_all();
    }
};


std::unique_ptr<IBlockInputStream> Aggregator::mergeAndConvertToBlocks(
    ManyAggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    if (data_variants.empty())
        throw Exception("Empty data passed to Aggregator::mergeAndConvertToBlocks.", ErrorCodes::EMPTY_DATA_PASSED);

    LOG_TRACE(log, "Merging aggregated data");

    ManyAggregatedDataVariants non_empty_data;
    non_empty_data.reserve(data_variants.size());
    for (auto & data : data_variants)
        if (!data->empty())
            non_empty_data.push_back(data);

    if (non_empty_data.empty())
        return std::make_unique<NullBlockInputStream>(getHeader(final));

    if (non_empty_data.size() > 1)
    {
        /// Sort the states in descending order so that the merge is more efficient (since all states are merged into the first).
        std::sort(non_empty_data.begin(), non_empty_data.end(),
            [](const AggregatedDataVariantsPtr & lhs, const AggregatedDataVariantsPtr & rhs)
            {
                return lhs->sizeWithoutOverflowRow() > rhs->sizeWithoutOverflowRow();
            });
    }

    /// If at least one of the options is two-level, then convert all the options into two-level ones, if there are not such.
    /// Note - perhaps it would be more optimal not to convert single-level versions before the merge, but merge them separately, at the end.

    bool has_at_least_one_two_level = false;
    for (const auto & variant : non_empty_data)
    {
        if (variant->isTwoLevel())
        {
            has_at_least_one_two_level = true;
            break;
        }
    }

    if (has_at_least_one_two_level)
        for (auto & variant : non_empty_data)
            if (!variant->isTwoLevel())
                variant->convertToTwoLevel();

    AggregatedDataVariantsPtr & first = non_empty_data[0];

    for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
    {
        if (first->type != non_empty_data[i]->type)
            throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

        /** Elements from the remaining sets can be moved to the first data set.
          * Therefore, it must own all the arenas of all other sets.
          */
        first->aggregates_pools.insert(first->aggregates_pools.end(),
            non_empty_data[i]->aggregates_pools.begin(), non_empty_data[i]->aggregates_pools.end());
    }

    return std::make_unique<MergingAndConvertingBlockInputStream>(*this, non_empty_data, final, max_threads);
}


template <bool no_more_keys, typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImplCase(
    Block & block,
    Arena * aggregates_pool,
    Method & method,
    Table & data,
    AggregateDataPtr overflow_row) const
{
    ColumnRawPtrs key_columns(params.keys_size);
    AggregateColumnsConstData aggregate_columns(params.aggregates_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i] = &typeid_cast<const ColumnAggregateFunction &>(*block.safeGetByPosition(params.keys_size + i).column).getData();

    typename Method::State state;
    state.init(key_columns);

    /// For all rows.
    StringRefs keys(params.keys_size);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        typename Table::iterator it;

        bool inserted;          /// Inserted a new key, or was this key already?
        bool overflow = false;  /// The new key did not fit in the hash table because of no_more_keys.

        /// Get the key to insert into the hash table.
        auto key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *aggregates_pool);

        if (!no_more_keys)
        {
            data.emplace(key, it, inserted);
        }
        else
        {
            inserted = false;
            it = data.find(key);
            if (data.end() == it)
                overflow = true;
        }

        /// If the key does not fit, and the data does not need to be aggregated into a separate row, then there's nothing to do.
        if (no_more_keys && overflow && !overflow_row)
        {
            method.onExistingKey(key, keys, *aggregates_pool);
            continue;
        }

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (inserted)
        {
            AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);
            aggregate_data = nullptr;

            method.onNewKey(*it, params.keys_size, keys, *aggregates_pool);

            AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(place);
            aggregate_data = place;
        }
        else
            method.onExistingKey(key, keys, *aggregates_pool);

        AggregateDataPtr value = (!no_more_keys || !overflow) ? Method::getAggregateData(it->second) : overflow_row;

        /// Merge state of aggregate functions.
        for (size_t j = 0; j < params.aggregates_size; ++j)
            aggregate_functions[j]->merge(
                value + offsets_of_aggregate_states[j],
                (*aggregate_columns[j])[i],
                aggregates_pool);
    }

    /// Early release memory.
    block.clear();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(
    Block & block,
    Arena * aggregates_pool,
    Method & method,
    Table & data,
    AggregateDataPtr overflow_row,
    bool no_more_keys) const
{
    if (!no_more_keys)
        mergeStreamsImplCase<false>(block, aggregates_pool, method, data, overflow_row);
    else
        mergeStreamsImplCase<true>(block, aggregates_pool, method, data, overflow_row);
}


void NO_INLINE Aggregator::mergeWithoutKeyStreamsImpl(
    Block & block,
    AggregatedDataVariants & result) const
{
    AggregateColumnsConstData aggregate_columns(params.aggregates_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i] = &typeid_cast<const ColumnAggregateFunction &>(*block.safeGetByPosition(params.keys_size + i).column).getData();

    AggregatedDataWithoutKey & res = result.without_key;
    if (!res)
    {
        AggregateDataPtr place = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        res = place;
    }

    /// Adding Values
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0], result.aggregates_pool);

    /// Early release memory.
    block.clear();
}


void Aggregator::mergeStream(const BlockInputStreamPtr & stream, AggregatedDataVariants & result, size_t max_threads)
{
    if (isCancelled())
        return;

    /** If the remote servers used a two-level aggregation method,
      *  then blocks will contain information about the number of the bucket.
      * Then the calculations can be parallelized by buckets.
      * We decompose the blocks to the bucket numbers indicated in them.
      */
    using BucketToBlocks = std::map<Int32, BlocksList>;
    BucketToBlocks bucket_to_blocks;

    /// Read all the data.
    LOG_TRACE(log, "Reading blocks of partially aggregated data.");

    size_t total_input_rows = 0;
    size_t total_input_blocks = 0;
    while (Block block = stream->read())
    {
        if (isCancelled())
            return;

        total_input_rows += block.rows();
        ++total_input_blocks;
        bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(block));
    }

    LOG_TRACE(log, "Read " << total_input_blocks << " blocks of partially aggregated data, total " << total_input_rows << " rows.");

    if (bucket_to_blocks.empty())
        return;

    /** `minus one` means the absence of information about the bucket
      * - in the case of single-level aggregation, as well as for blocks with "overflowing" values.
      * If there is at least one block with a bucket number greater than zero, then there was a two-level aggregation.
      */
    auto max_bucket = bucket_to_blocks.rbegin()->first;
    size_t has_two_level = max_bucket >= 0;

    if (has_two_level)
    {
    #define M(NAME) \
        if (method_chosen == AggregatedDataVariants::Type::NAME) \
            method_chosen = AggregatedDataVariants::Type::NAME ## _two_level;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M
    }

    if (isCancelled())
        return;

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(method_chosen);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    bool has_blocks_with_unknown_bucket = bucket_to_blocks.count(-1);

    /// First, parallel the merge for the individual buckets. Then we continue merge the data not allocated to the buckets.
    if (has_two_level)
    {
        /** In this case, no_more_keys is not supported due to the fact that
          *  from different threads it is difficult to update the general state for "other" keys (overflows).
          * That is, the keys in the end can be significantly larger than max_rows_to_group_by.
          */

        LOG_TRACE(log, "Merging partially aggregated two-level data.");

        auto merge_bucket = [&bucket_to_blocks, &result, this](Int32 bucket, Arena * aggregates_pool, ThreadGroupStatusPtr thread_group)
        {
            CurrentThread::attachToIfDetached(thread_group);

            for (Block & block : bucket_to_blocks[bucket])
            {
                if (isCancelled())
                    return;

            #define M(NAME) \
                else if (result.type == AggregatedDataVariants::Type::NAME) \
                    mergeStreamsImpl(block, aggregates_pool, *result.NAME, result.NAME->data.impls[bucket], nullptr, false);

                if (false) {}
                    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
            #undef M
                else
                    throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
            }
        };

        std::unique_ptr<ThreadPool> thread_pool;
        if (max_threads > 1 && total_input_rows > 100000)    /// TODO Make a custom threshold.
            thread_pool = std::make_unique<ThreadPool>(max_threads);

        for (const auto & bucket_blocks : bucket_to_blocks)
        {
            const auto bucket = bucket_blocks.first;

            if (bucket == -1)
                continue;

            result.aggregates_pools.push_back(std::make_shared<Arena>());
            Arena * aggregates_pool = result.aggregates_pools.back().get();

            auto task = std::bind(merge_bucket, bucket, aggregates_pool, CurrentThread::getGroup());

            if (thread_pool)
                thread_pool->schedule(task);
            else
                task();
        }

        if (thread_pool)
            thread_pool->wait();

        LOG_TRACE(log, "Merged partially aggregated two-level data.");
    }

    if (isCancelled())
    {
        result.invalidate();
        return;
    }

    if (has_blocks_with_unknown_bucket)
    {
        LOG_TRACE(log, "Merging partially aggregated single-level data.");

        bool no_more_keys = false;

        BlocksList & blocks = bucket_to_blocks[-1];
        for (Block & block : blocks)
        {
            if (isCancelled())
            {
                result.invalidate();
                return;
            }

            if (!checkLimits(result.sizeWithoutOverflowRow(), no_more_keys))
                break;

            if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
                mergeWithoutKeyStreamsImpl(block, result);

        #define M(NAME, IS_TWO_LEVEL) \
            else if (result.type == AggregatedDataVariants::Type::NAME) \
                mergeStreamsImpl(block, result.aggregates_pool, *result.NAME, result.NAME->data, result.without_key, no_more_keys);

            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
            else if (result.type != AggregatedDataVariants::Type::without_key)
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }

        LOG_TRACE(log, "Merged partially aggregated single-level data.");
    }
}


Block Aggregator::mergeBlocks(BlocksList & blocks, bool final)
{
    if (blocks.empty())
        return {};

    auto bucket_num = blocks.front().info.bucket_num;
    bool is_overflows = blocks.front().info.is_overflows;

    LOG_TRACE(log, "Merging partially aggregated blocks (bucket = " << bucket_num << ").");
    Stopwatch watch;

    /** If possible, change 'method' to some_hash64. Otherwise, leave as is.
      * Better hash function is needed because during external aggregation,
      *  we may merge partitions of data with total number of keys far greater than 4 billion.
      */
    auto merge_method = method_chosen;

#define APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M) \
        M(key64)            \
        M(key_string)       \
        M(key_fixed_string) \
        M(keys128)          \
        M(keys256)          \
        M(serialized)       \

#define M(NAME) \
    if (merge_method == AggregatedDataVariants::Type::NAME) \
        merge_method = AggregatedDataVariants::Type::NAME ## _hash64; \

    APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M)
#undef M

#undef APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION

    /// Temporary data for aggregation.
    AggregatedDataVariants result;

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(merge_method);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    for (Block & block : blocks)
    {
        if (isCancelled())
            return {};

        if (bucket_num >= 0 && block.info.bucket_num != bucket_num)
            bucket_num = -1;

        if (result.type == AggregatedDataVariants::Type::without_key || is_overflows)
            mergeWithoutKeyStreamsImpl(block, result);

    #define M(NAME, IS_TWO_LEVEL) \
        else if (result.type == AggregatedDataVariants::Type::NAME) \
            mergeStreamsImpl(block, result.aggregates_pool, *result.NAME, result.NAME->data, nullptr, false);

        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
        else if (result.type != AggregatedDataVariants::Type::without_key)
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    if (isCancelled())
        return {};

    Block block;
    if (result.type == AggregatedDataVariants::Type::without_key || is_overflows)
        block = prepareBlockAndFillWithoutKey(result, final, is_overflows);
    else
        block = prepareBlockAndFillSingleLevel(result, final);
    /// NOTE: two-level data is not possible here - chooseAggregationMethod chooses only among single-level methods.

    if (!final)
    {
        /// Pass ownership of aggregate function states from result to ColumnAggregateFunction objects in the resulting block.
        result.aggregator = nullptr;
    }

    size_t rows = block.rows();
    size_t bytes = block.bytes();
    double elapsed_seconds = watch.elapsedSeconds();
    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Merged partially aggregated blocks. "
        << rows << " rows, " << bytes / 1048576.0 << " MiB."
        << " in " << elapsed_seconds << " sec."
        << " (" << rows / elapsed_seconds << " rows/sec., " << bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

    if (isCancelled())
        return {};

    block.info.bucket_num = bucket_num;
    return block;
}


template <typename Method>
void NO_INLINE Aggregator::convertBlockToTwoLevelImpl(
    Method & method,
    Arena * pool,
    ColumnRawPtrs & key_columns,
    StringRefs & keys,
    const Block & source,
    std::vector<Block> & destinations) const
{
    typename Method::State state;
    state.init(key_columns);

    size_t rows = source.rows();
    size_t columns = source.columns();

    /// Create a 'selector' that will contain bucket index for every row. It will be used to scatter rows to buckets.
    IColumn::Selector selector(rows);

    /// For every row.
    for (size_t i = 0; i < rows; ++i)
    {
        /// Obtain a key. Calculate bucket number from it.
        typename Method::Key key = state.getKey(key_columns, params.keys_size, i, key_sizes, keys, *pool);

        auto hash = method.data.hash(key);
        auto bucket = method.data.getBucketFromHash(hash);

        selector[i] = bucket;

        /// We don't need to store this key in pool.
        method.onExistingKey(key, keys, *pool);
    }

    size_t num_buckets = destinations.size();

    for (size_t column_idx = 0; column_idx < columns; ++column_idx)
    {
        const ColumnWithTypeAndName & src_col = source.getByPosition(column_idx);
        MutableColumns scattered_columns = src_col.column->scatter(num_buckets, selector);

        for (size_t bucket = 0, size = num_buckets; bucket < size; ++bucket)
        {
            if (!scattered_columns[bucket]->empty())
            {
                Block & dst = destinations[bucket];
                dst.info.bucket_num = bucket;
                dst.insert({std::move(scattered_columns[bucket]), src_col.type, src_col.name});
            }

            /** Inserted columns of type ColumnAggregateFunction will own states of aggregate functions
              *  by holding shared_ptr to source column. See ColumnAggregateFunction.h
              */
        }
    }
}


std::vector<Block> Aggregator::convertBlockToTwoLevel(const Block & block)
{
    if (!block)
        return {};

    AggregatedDataVariants data;

    StringRefs key(params.keys_size);
    ColumnRawPtrs key_columns(params.keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    AggregatedDataVariants::Type type = method_chosen;
    data.keys_size = params.keys_size;
    data.key_sizes = key_sizes;

#define M(NAME) \
    else if (type == AggregatedDataVariants::Type::NAME) \
        type = AggregatedDataVariants::Type::NAME ## _two_level;

    if (false) {}
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    data.init(type);

    size_t num_buckets = 0;

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        num_buckets = data.NAME->data.NUM_BUCKETS;

    if (false) {}
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    std::vector<Block> splitted_blocks(num_buckets);

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        convertBlockToTwoLevelImpl(*data.NAME, data.aggregates_pool, \
            key_columns, key, block, splitted_blocks);

    if (false) {}
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    return splitted_blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::destroyImpl(Table & table) const
{
    for (auto elem : table)
    {
        AggregateDataPtr & data = Method::getAggregateData(elem.second);

        /** If an exception (usually a lack of memory, the MemoryTracker throws) arose
          *  after inserting the key into a hash table, but before creating all states of aggregate functions,
          *  then data will be equal nullptr.
          */
        if (nullptr == data)
            continue;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            if (!aggregate_functions[i]->isState())
                aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);

        data = nullptr;
    }
}


void Aggregator::destroyWithoutKey(AggregatedDataVariants & result) const
{
    AggregatedDataWithoutKey & res_data = result.without_key;

    if (nullptr != res_data)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            if (!aggregate_functions[i]->isState())
                aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);

        res_data = nullptr;
    }
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
    if (result.size() == 0)
        return;

    LOG_TRACE(log, "Destroying aggregate states");

    /// In what data structure is the data aggregated?
    if (result.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
        destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        destroyImpl<decltype(result.NAME)::element_type>(result.NAME->data);

    if (false) {}
    APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    else if (result.type != AggregatedDataVariants::Type::without_key)
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


void Aggregator::setCancellationHook(const CancellationHook cancellation_hook)
{
    isCancelled = cancellation_hook;
}


}
