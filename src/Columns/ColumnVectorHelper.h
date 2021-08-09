#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>


namespace DB
{

/** Allows to access internal array of ColumnVector or ColumnFixedString without cast to concrete type.
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
class ColumnVectorHelper : public IColumn
{
public:
    template <size_t ELEMENT_SIZE>
    const char * getRawDataBegin() const
    {
        return reinterpret_cast<const PODArrayBase<ELEMENT_SIZE, 4096, Allocator<false>, 15, 16> *>(reinterpret_cast<const char *>(this) + sizeof(*this))->raw_data();
    }

    template <size_t ELEMENT_SIZE>
    void insertRawData(const char * ptr)
    {
        return reinterpret_cast<PODArrayBase<ELEMENT_SIZE, 4096, Allocator<false>, 15, 16> *>(reinterpret_cast<char *>(this) + sizeof(*this))->push_back_raw(ptr);
    }
};

}
