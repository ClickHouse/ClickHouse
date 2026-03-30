#pragma once

#include <bit>
#include <cstddef>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Buzhash (cyclic polynomial hash) implementation for rolling hash.
  * Used for content-defined chunking (CDC)
  *
  * Formula: H = XOR_{j=0}^{W-1} rotl(T[b_j], W-1-j)
  * Rolling update: H_new = rotl(H_old, 1) ^ rotl(T[b_out], W mod 64) ^ T[b_in]
  */
class BuzhashImpl
{
public:
    static constexpr size_t HASH_BITS = 64;
    static constexpr size_t MIN_WINDOW_SIZE = 1;
    static constexpr size_t MAX_WINDOW_SIZE = 256;

    BuzhashImpl() = default;

    /// Compute initial hash for the first window of size window_size.
    static UInt64 computeInitialHash(const UInt8 * data, size_t window_size)
    {
        UInt64 hash = 0;
        for (size_t j = 0; j < window_size; ++j)
        {
            UInt64 v = getTable()[data[j]];
            hash ^= std::rotl(v, static_cast<unsigned>(window_size - 1 - j) % HASH_BITS);
        }
        return hash;
    }

    /// Rolling update: remove byte at position 0, add new byte.
    static UInt64 roll(UInt64 hash, UInt8 out_byte, UInt8 in_byte, size_t window_size)
    {
        const unsigned rot_out = static_cast<unsigned>(window_size % HASH_BITS);
        return std::rotl(hash, 1)
            ^ std::rotl(getTable()[out_byte], rot_out)
            ^ getTable()[in_byte];
    }

private:
    /// Lookup table T[256] for Buzhash (initialized in FunctionsRollingHash.cpp).
    static const UInt64 * getTable();
};

/** Content-defined chunking (Buzhash + hash % reverse_probability). */
namespace RollingHashCDC
{

size_t maxChunkSizeForCdc(UInt64 reverse_probability);
bool isUtf8ChunkBoundary(const UInt8 * data, size_t byte_pos, size_t data_size);
size_t forceCutPositionBytes(const UInt8 * data, size_t data_size, size_t chunk_start, size_t max_chunk_size);
size_t forceCutPositionUtf8(const UInt8 * data, size_t data_size, size_t chunk_start, size_t max_chunk_size);

template <typename EmitChunk, typename EmitOffset>
void forEachContentDefinedChunk(
    const UInt8 * data,
    size_t data_size,
    size_t window_size,
    UInt64 reverse_probability,
    bool utf8_boundaries,
    bool collect_offsets,
    EmitChunk && emit_chunk,
    EmitOffset && emit_offset)
{
    const size_t min_chunk_size = window_size;
    const size_t max_chunk_size = maxChunkSizeForCdc(reverse_probability);

    if (data_size == 0)
        return;

    if (collect_offsets)
        emit_offset(0);

    if (data_size < window_size)
    {
        emit_chunk(reinterpret_cast<const char *>(data), data_size);
        return;
    }

    size_t chunk_start = 0;
    size_t window_end = window_size;
    UInt64 hash = BuzhashImpl::computeInitialHash(data, window_size);

    while (true)
    {
        const size_t chunk_len = window_end - chunk_start;

        if (chunk_len >= max_chunk_size)
        {
            const size_t cut_end = utf8_boundaries
                ? forceCutPositionUtf8(data, data_size, chunk_start, max_chunk_size)
                : forceCutPositionBytes(data, data_size, chunk_start, max_chunk_size);

            emit_chunk(reinterpret_cast<const char *>(data + chunk_start), cut_end - chunk_start);
            if (collect_offsets && cut_end < data_size)
                emit_offset(cut_end);

            chunk_start = cut_end;
            if (chunk_start >= data_size)
                return;
            if (data_size - chunk_start < window_size)
            {
                emit_chunk(reinterpret_cast<const char *>(data + chunk_start), data_size - chunk_start);
                return;
            }
            hash = BuzhashImpl::computeInitialHash(data + chunk_start, window_size);
            window_end = chunk_start + window_size;
            continue;
        }

        const bool hash_boundary = (hash % reverse_probability) == 0;
        const bool utf8_ok = !utf8_boundaries || isUtf8ChunkBoundary(data, window_end, data_size);
        if (chunk_len >= min_chunk_size && hash_boundary && utf8_ok)
        {
            emit_chunk(reinterpret_cast<const char *>(data + chunk_start), chunk_len);
            if (collect_offsets && window_end < data_size)
                emit_offset(window_end);

            chunk_start = window_end;
            if (chunk_start >= data_size)
                return;
            if (data_size - chunk_start < window_size)
            {
                emit_chunk(reinterpret_cast<const char *>(data + chunk_start), data_size - chunk_start);
                return;
            }
            hash = BuzhashImpl::computeInitialHash(data + chunk_start, window_size);
            window_end = chunk_start + window_size;
            continue;
        }

        if (window_end == data_size)
        {
            emit_chunk(reinterpret_cast<const char *>(data + chunk_start), data_size - chunk_start);
            return;
        }

        hash = BuzhashImpl::roll(hash, data[window_end - window_size], data[window_end], window_size);
        ++window_end;
    }
}

}

/** Content-defined chunking (CDC): Buzhash rolling hash; cut when hash is divisible by reverse_probability
  * (expected ~reverse_probability bytes between boundaries on average). Min/max chunk sizes are enforced internally.
  */
class FunctionContentDefinedChunks : public IFunction
{
public:
    static constexpr auto name = "contentDefinedChunks";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionContentDefinedChunks>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForVariant() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

class FunctionContentDefinedChunksUTF8 : public IFunction
{
public:
    static constexpr auto name = "contentDefinedChunksUTF8";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionContentDefinedChunksUTF8>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForVariant() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

class FunctionContentDefinedChunkOffsets : public IFunction
{
public:
    static constexpr auto name = "contentDefinedChunkOffsets";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionContentDefinedChunkOffsets>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForVariant() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

class FunctionContentDefinedChunkOffsetsUTF8 : public IFunction
{
public:
    static constexpr auto name = "contentDefinedChunkOffsetsUTF8";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionContentDefinedChunkOffsetsUTF8>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForVariant() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
