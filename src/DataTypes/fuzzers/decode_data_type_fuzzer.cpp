#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <Core/Field.h>
#include <Columns/IColumn.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;

ContextMutablePtr context;
extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerAggregateFunctions();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// Input: [0] = selector byte, [1..] = raw bytes interpreted as a BinaryTypeIndex-encoded
        /// data type, optionally followed by a binary value of that type.
        ///
        /// Selector bit 0 picks the type-complexity limit passed to `decodeDataType` and propagated
        /// into `FormatSettings::binary.max_binary_type_complexity`. `0` is the trusted, unlimited
        /// mode the storage layer uses for already-stored data; `1000` is the production default of
        /// `input_format_binary_max_type_complexity` that input formats apply to untrusted input.
        /// Without a non-zero limit the rejection branch in `decodeDataTypeImpl` is unreachable, so
        /// both shapes are fuzzed explicitly.
        ///
        /// Compile the code as follows:
        ///   mkdir build_asan_fuzz
        ///   cd build_asan_fuzz
        ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
        ///
        /// The fuzzer can be run as follows:
        ///   ../../../build_asan_fuzz/src/DataTypes/fuzzers/decode_data_type_fuzzer corpus \
        ///       -dict=../../../tests/fuzz/dictionaries/binary_types.dict -jobs=8

        if (size < 1)
            return 0;

        const bool limit_binary_type_complexity = (data[0] & 1) != 0;
        const size_t max_binary_type_complexity = limit_binary_type_complexity ? 1000 : 0;

        DB::ReadBufferFromMemory in(data + 1, size - 1);

        DataTypePtr type = decodeDataType(in, max_binary_type_complexity);

        /// Exercise the type's default serialization to increase coverage.
        auto serialization = type->getDefaultSerialization();
        auto column = type->createColumn();

        /// Try deserializing a single binary value from the remaining buffer to
        /// exercise the serialization path without requiring valid data.
        FormatSettings settings;
        settings.binary.max_binary_array_size = 100;
        settings.binary.max_binary_string_size = 100;
        settings.binary.max_binary_type_complexity = max_binary_type_complexity;

        if (!in.eof())
        {
            Field field;
            serialization->deserializeBinary(field, in, settings);
        }
    }
    catch (...)
    {
    }

    return 0;
}
