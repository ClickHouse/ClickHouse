#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <Common/Arena.h>


namespace DB
{

/** Allows to access internal array of fixed-size column without cast to concrete type.
  * We will inherit ColumnVector and ColumnFixedString from this class instead of IColumn.
  * Assumes data layout of ColumnVector, ColumnFixedString and PODArray.
  *
  * Why it is needed?
  *
  * There are some algorithms that specialize on the size of data type but doesn't care about concrete type.
  * The same specialization may work for UInt64, Int64, Float64, FixedString(8), if it only does byte moving and hashing.
  * To avoid code bloat and compile time increase, we can use single template instantiation for these cases
  *  and just static_cast pointer to some single column type (e. g. ColumnUInt64) assuming that all types have identical memory layout.
  *
  * But this static_cast (downcast to unrelated type) is illegal according to the C++ standard and UBSan warns about it.
  * To allow functional tests to work under UBSan we have to separate some base class that will present the memory layout in explicit way,
  *  and we will do static_cast to this class.
  */
class ColumnFixedSizeHelper : public IColumn
{
    template <size_t element_size>
    using PODArrayBaseClass = PODArrayBase<element_size, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

public:
    template <size_t element_size>
    const char * getRawDataBegin() const
    {
        return reinterpret_cast<const PODArrayBaseClass<element_size> *>(reinterpret_cast<const char *>(this) + sizeof(*this))->raw_data();
    }

    template <size_t element_size>
    void insertRawData(const char * ptr)
    {
        return reinterpret_cast<PODArrayBaseClass<element_size> *>(reinterpret_cast<char *>(this) + sizeof(*this))->push_back_raw(ptr);
    }

    char * serializeValueIntoMemory(size_t n, char * memory, const SerializationSettings *) const final
    {
        const char * raw_data_begin = getRawDataBegin<1>() + n * fixed_size;
        memcpy(memory, raw_data_begin, fixed_size);
        return memory + fixed_size;
    }

    char *
    serializeValueIntoMemoryWithNull(size_t n, char * memory, const UInt8 * is_null, const SerializationSettings * settings) const final
    {
        if (is_null)
        {
            *memory = is_null[n];
            ++memory;
            if (is_null[n])
                return memory;
        }

        return this->serializeValueIntoMemory(n, memory, settings);
    }

    std::string_view
    serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const SerializationSettings * settings) const final
    {
        char * memory = arena.allocContinue(fixed_size, begin);
        this->serializeValueIntoMemory(n, memory, settings);
        return {memory, fixed_size};
    }

    std::string_view serializeValueIntoArenaWithNull(
        size_t n, Arena & arena, char const *& begin, const UInt8 * is_null, const SerializationSettings * settings) const final
    {
        if (is_null)
        {
            char * memory;
            if (is_null[n])
            {
                memory = arena.allocContinue(1, begin);
                *memory = 1;
                return {memory, 1};
            }

            auto serialized_value_size = this->getSerializedValueSize(n, settings);
            if (serialized_value_size)
            {
                size_t total_size = *serialized_value_size + 1 /* null map byte */;
                memory = arena.allocContinue(total_size, begin);
                *memory = 0;
                this->serializeValueIntoMemory(n, memory + 1, settings);
                return {memory, total_size};
            }

            memory = arena.allocContinue(1, begin);
            *memory = 0;
            auto res = this->serializeValueIntoArena(n, arena, begin, settings);
            return std::string_view(res.data() - 1, res.size() + 1);
        }

        return this->serializeValueIntoArena(n, arena, begin, settings);
    }

    void setFixedSize(size_t size) { fixed_size = size; }

    size_t fixed_size;
};

}
