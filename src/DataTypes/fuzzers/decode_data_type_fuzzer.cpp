#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Core/Field.h>
#include <Columns/IColumn.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;

ContextMutablePtr context;
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

        /// Input: raw bytes interpreted as a BinaryTypeIndex-encoded data type.
        ///
        /// Compile the code as follows:
        ///   mkdir build_asan_fuzz
        ///   cd build_asan_fuzz
        ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
        ///
        /// The fuzzer can be run as follows:
        ///   ../../../build_asan_fuzz/src/DataTypes/fuzzers/decode_data_type_fuzzer corpus \
        ///       -dict=../../../tests/fuzz/dictionaries/binary_types.dict -jobs=8

        DB::ReadBufferFromMemory in(data, size);

        DataTypePtr type = decodeDataType(in);

        /// Exercise the type's default serialization to increase coverage.
        auto serialization = type->getDefaultSerialization();
        auto column = type->createColumn();

        /// Try deserializing a single binary value from the remaining buffer to
        /// exercise the serialization path without requiring valid data.
        FormatSettings settings;
        settings.binary.max_binary_array_size = 100;
        settings.binary.max_binary_string_size = 100;

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
