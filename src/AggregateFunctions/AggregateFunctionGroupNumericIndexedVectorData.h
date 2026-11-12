#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/PODArray.h>

/// Include this last — see the reason inside
#include <AggregateFunctions/AggregateFunctionGroupNumericIndexedVectorDataBSI.h>

namespace DB
{

#define FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64)

/** `NumericIndexedVector` encapsulates the vector (in mathematics) data structure and exposes the following operations.
 * Operations on a single vector:
 *  - addValue(index, value): v[index] += value;
 *  - getAllValueSum(): Get the sum of all elements of a vector;
 *  - getCardinality(): Get the number of non-zero values ​​in a vector
 *  - ......
 * Operations between two vectors:
 *  - pointwiseAdd(v1, v2, res): Pointwise addition of v1 and v2, result stored in res vector.
 *      Similar operations include pointwise subtraction, multiplication, comparison, etc.
 *  - ......
 *
 *  `VectorImpl` is the implementation of vector, including how to store and implement the operations provided above on this storage. For one of the specific implementations, see the `BSINumericIndexedVector`.
 */
template <typename VectorImpl>
class NumericIndexedVector
{
private:
    VectorImpl impl;

    /** Use IndexType to indicate the type of the subscript. It determines the length of the vector.
     * If it is UInt8, the maximum length of the vector is 256, and
     * if it is UInt32, the maximum length is 4,294,967,296.
     * The supported index types are defined in FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES
     */
    using IndexType = typename VectorImpl::IndexType;

    /// The supported value types are defined in FOR_BASIC_NUMERIC_TYPES.
    using ValueType = typename VectorImpl::ValueType;

public:
    template <typename... TArgs>
    void initialize(TArgs... args)
    {
        impl.initialize(std::forward<TArgs>(args)...);
    }
    void deepCopyFrom(const NumericIndexedVector & rhs) { impl.deepCopyFrom(rhs.impl); }

    void merge(const NumericIndexedVector & rhs) { impl.merge(rhs.impl); }

    /// Performs pointwise addition between two vectors.
    static void pointwiseAdd(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseAdd(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseAdd(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseAdd(lhs.impl, rhs, res.impl);
    }

    /// Performs pointwise subtraction between two vectors.
    static void pointwiseSubtract(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseSubtract(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseSubtract(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseSubtract(lhs.impl, rhs, res.impl);
    }

    /// Performs pointwise multiplication between two vectors.
    static void pointwiseMultiply(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseMultiply(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseMultiply(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseMultiply(lhs.impl, rhs, res.impl);
    }

    /// Performs pointwise division between two vectors.
    static void pointwiseDivide(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseDivide(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseDivide(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseDivide(lhs.impl, rhs, res.impl);
    }

    /** Performs pointwise equality between two vectors.
     * The result is a vector with all non-zero value is 1.
     */
    static void pointwiseEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseEqual(lhs.impl, rhs, res.impl);
    }

    static void pointwiseNotEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseNotEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseNotEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseNotEqual(lhs.impl, rhs, res.impl);
    }

    static void pointwiseLess(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLess(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseLess(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLess(lhs.impl, rhs, res.impl);
    }

    static void pointwiseLessEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLessEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseLessEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLessEqual(lhs.impl, rhs, res.impl);
    }

    static void pointwiseGreater(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreater(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseGreater(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreater(lhs.impl, rhs, res.impl);
    }

    static void pointwiseGreaterEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreaterEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseGreaterEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreaterEqual(lhs.impl, rhs, res.impl);
    }

    void addValue(IndexType index, ValueType value) { impl.addValue(index, value); }

    ValueType getValue(IndexType index) const { return impl.getValue(index); }

    Float64 getAllValueSum() const { return impl.getAllValueSum(); }

    UInt64 getCardinality() const { return impl.getCardinality(); }

    String shortDebugString() const { return impl.shortDebugString(); }

    UInt64 toIndexValueMap(PaddedPODArray<IndexType> & indexes_pod, PaddedPODArray<ValueType> & values_pod) const
    {
        return impl.toIndexValueMap(indexes_pod, values_pod);
    }

    void read(DB::ReadBuffer & in) { impl.read(in); }

    void write(DB::WriteBuffer & out) const { impl.write(out); }
};

struct NameAggregateFunctionGroupNumericIndexedVector
{
    static constexpr auto name = "groupNumericIndexedVector";
};

template <typename VectorImpl>
struct AggregateFunctionGroupNumericIndexedVectorData
{
    bool init = false;
    NumericIndexedVector<VectorImpl> vector;
    static const char * name() { return NameAggregateFunctionGroupNumericIndexedVector::name; }
};

}
