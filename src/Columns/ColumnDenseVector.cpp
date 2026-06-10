#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnDenseVector.h>
#include <Columns/ColumnsCommon.h>
#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/iota.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SIZES_OF_ARRAYS_DONT_MATCH;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


ColumnDenseVector::ColumnDenseVector(MutableColumnPtr && nested_, TypeIndex element_type_, size_t dimension_)
    : nested(std::move(nested_))
    , element_type(element_type_)
    , dimension(dimension_)
{
    if (dimension == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Dimension of dense vector column must be positive");

    /// dispatchByElementType also rejects element types outside of {BFloat16, Float32, Float64}.
    dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        if (!typeid_cast<const ColumnVector<T> *>(nested.get()))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Dense vector column expected ColumnVector({}) as the nested column, got {}",
                TypeName<T>,
                nested->getName());
    });

    if (nested->size() % dimension != 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Size of the nested column ({}) is not a multiple of the dense vector dimension ({})",
            nested->size(),
            dimension);
}

std::string ColumnDenseVector::getName() const
{
    return fmt::format("Vector({}, {})", elementTypeName(element_type), dimension);
}

MutableColumnPtr ColumnDenseVector::cloneResized(size_t new_size) const
{
    return ColumnDenseVector::create(nested->cloneResized(new_size * dimension), element_type, dimension);
}

const ColumnDenseVector & ColumnDenseVector::checkCompatibility(const IColumn & src) const
{
    const auto & src_vec = assert_cast<const ColumnDenseVector &>(src);
    if (src_vec.element_type != element_type || src_vec.dimension != dimension)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot insert values of dense vector column {} into dense vector column {}",
            src_vec.getName(),
            getName());
    return src_vec;
}

Field ColumnDenseVector::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnDenseVector::get(size_t n, Field & res) const
{
    res = Array();
    Array & res_arr = res.safeGet<Array>();
    res_arr.reserve(dimension);

    const size_t offset = n * dimension;
    for (size_t i = 0; i < dimension; ++i)
        res_arr.push_back((*nested)[offset + i]);
}

void ColumnDenseVector::getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    const size_t offset = n * dimension;

    if (options.notFull(name_buf))
        name_buf << "[";

    for (size_t i = 0; i < dimension; ++i)
    {
        if (options.notFull(name_buf) && i > 0)
            name_buf << ", ";
        nested->getValueNameImpl(name_buf, offset + i, options);
        if (!options.notFull(name_buf))
            break;
    }

    if (options.notFull(name_buf))
        name_buf << "]";
}

void ColumnDenseVector::insertData(const char * pos, size_t length)
{
    const size_t value_size = getValueSize();
    if (length != value_size)
        throw Exception(
            ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
            "Cannot insert data of size {} into dense vector column with value size {}",
            length,
            value_size);

    dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = getTypedData<T>();
        const size_t old_size = data.size();
        data.resize(old_size + dimension);
        memcpy(&data[old_size], pos, value_size);
    });
}

void ColumnDenseVector::insert(const Field & x)
{
    const Array & array = x.safeGet<Array>();
    if (array.size() != dimension)
        throw Exception(
            ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
            "Cannot insert array of size {} into dense vector column with dimension {}",
            array.size(),
            dimension);

    for (size_t i = 0; i < dimension; ++i)
        nested->insert(array[i]);
}

bool ColumnDenseVector::tryInsert(const Field & x)
{
    if (x.getType() != Field::Types::Which::Array)
        return false;

    const Array & array = x.safeGet<Array>();
    if (array.size() != dimension)
        return false;

    for (size_t i = 0; i < dimension; ++i)
    {
        if (!nested->tryInsert(array[i]))
        {
            nested->popBack(i);
            return false;
        }
    }
    return true;
}

bool ColumnDenseVector::isDefaultAt(size_t n) const
{
    const auto value = getDataAt(n);
    return memoryIsZero(value.data(), 0, value.size());
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDenseVector::insertFrom(const IColumn & src_, size_t n)
#else
void ColumnDenseVector::doInsertFrom(const IColumn & src_, size_t n)
#endif
{
    const auto & src = checkCompatibility(src_);
    nested->insertRangeFrom(src.getNestedColumn(), n * dimension, dimension);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDenseVector::insertManyFrom(const IColumn & src_, size_t position, size_t length)
#else
void ColumnDenseVector::doInsertManyFrom(const IColumn & src_, size_t position, size_t length)
#endif
{
    const auto & src = checkCompatibility(src_);
    dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = getTypedData<T>();
        const auto & src_data = src.getTypedData<T>();
        const size_t old_size = data.size();
        data.resize(old_size + length * dimension);
        for (size_t i = 0; i < length; ++i)
            memcpy(&data[old_size + i * dimension], &src_data[position * dimension], dimension * sizeof(T));
    });
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDenseVector::insertRangeFrom(const IColumn & src_, size_t start, size_t length)
#else
void ColumnDenseVector::doInsertRangeFrom(const IColumn & src_, size_t start, size_t length)
#endif
{
    const auto & src = checkCompatibility(src_);
    nested->insertRangeFrom(src.getNestedColumn(), start * dimension, length * dimension);
}

int ColumnDenseVector::compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
{
    const auto & rhs = assert_cast<const ColumnDenseVector &>(rhs_);
    for (size_t i = 0; i < dimension; ++i)
    {
        int res = nested->compareAt(n * dimension + i, m * dimension + i, rhs.getNestedColumn(), nan_direction_hint);
        if (res)
            return res;
    }
    return 0;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnDenseVector::compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
#else
int ColumnDenseVector::doCompareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
#endif
{
    return compareAtImpl(n, m, rhs_, nan_direction_hint);
}

std::string_view ColumnDenseVector::serializeValueIntoArena(
    size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings *) const
{
    const size_t value_size = getValueSize();
    char * memory = arena.allocContinue(value_size, begin);
    memcpy(memory, getDataAt(n).data(), value_size);
    return {memory, value_size};
}

char * ColumnDenseVector::serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings *) const
{
    const size_t value_size = getValueSize();
    memcpy(memory, getDataAt(n).data(), value_size);
    return memory + value_size;
}

void ColumnDenseVector::deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings *)
{
    dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = getTypedData<T>();
        const size_t old_size = data.size();
        data.resize(old_size + dimension);
        in.readStrict(reinterpret_cast<char *>(&data[old_size]), dimension * sizeof(T));
    });
}

void ColumnDenseVector::skipSerializedInArena(ReadBuffer & in) const
{
    in.ignore(getValueSize());
}

void ColumnDenseVector::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto value = getDataAt(n);
    hash.update(value.data(), value.size());
}

void ColumnDenseVector::updateHashWithValueRange(size_t begin, size_t end, SipHash & hash) const
{
    const size_t value_size = getValueSize();
    hash.update(nested->getRawData().data() + begin * value_size, (end - begin) * value_size);
}

WeakHash32 ColumnDenseVector::getWeakHash32() const
{
    const size_t s = size();
    WeakHash32 hash(s);

    const size_t value_size = getValueSize();
    const auto * pos = reinterpret_cast<const UInt8 *>(nested->getRawData().data());

    for (size_t row = 0; row < s; ++row)
    {
        hash.getData()[row] = ::updateWeakHash32(pos, value_size, hash.getData()[row]);
        pos += value_size;
    }

    return hash;
}

void ColumnDenseVector::expand(const Filter & mask, bool inverted)
{
    if (mask.size() < size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mask size should be no less than data size.");

    dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = getTypedData<T>();

        ssize_t index = mask.size() - 1;
        ssize_t from = size() - 1;
        data.resize_fill(mask.size() * dimension);
        while (index >= 0)
        {
            if (!!mask[index] ^ inverted)
            {
                if (from < 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many rows in mask");

                memcpy(&data[index * dimension], &data[from * dimension], dimension * sizeof(T));
                --from;
            }

            --index;
        }

        if (from != -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not enough rows in mask");
    });
}

ColumnPtr ColumnDenseVector::filter(const Filter & filt, ssize_t result_size_hint) const
{
    const size_t col_size = size();
    if (col_size != filt.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), col_size);

    return dispatchByElementType(element_type, [&](auto tag) -> ColumnPtr
    {
        using T = decltype(tag);
        const auto & data = getTypedData<T>();

        auto res_nested = ColumnVector<T>::create();
        auto & res_data = res_nested->getData();
        if (result_size_hint)
            res_data.reserve_exact(result_size_hint > 0 ? result_size_hint * dimension : data.size());

        for (size_t row = 0; row < col_size; ++row)
        {
            if (filt[row])
            {
                const size_t old_size = res_data.size();
                res_data.resize(old_size + dimension);
                memcpy(&res_data[old_size], &data[row * dimension], dimension * sizeof(T));
            }
        }

        MutableColumnPtr res_column = std::move(res_nested);
        return ColumnDenseVector::create(std::move(res_column), element_type, dimension);
    });
}

void ColumnDenseVector::filter(const Filter & filt)
{
    const size_t col_size = size();
    if (col_size != filt.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), col_size);

    dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = getTypedData<T>();

        size_t result_size = 0;
        for (size_t row = 0; row < col_size; ++row)
        {
            if (filt[row])
            {
                if (result_size != row)
                    memmove(&data[result_size * dimension], &data[row * dimension], dimension * sizeof(T));
                ++result_size;
            }
        }

        data.resize_assume_reserved(result_size * dimension);
    });
}

ColumnPtr ColumnDenseVector::permute(const Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
}

ColumnPtr ColumnDenseVector::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnDenseVector::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    chassert(limit <= indexes.size());

    return dispatchByElementType(element_type, [&](auto tag) -> ColumnPtr
    {
        using T = decltype(tag);
        const auto & data = getTypedData<T>();

        auto res_nested = ColumnVector<T>::create();
        auto & res_data = res_nested->getData();
        res_data.resize(limit * dimension);

        for (size_t i = 0; i < limit; ++i)
            memcpy(&res_data[i * dimension], &data[indexes[i] * dimension], dimension * sizeof(T));

        MutableColumnPtr res_column = std::move(res_nested);
        return ColumnDenseVector::create(std::move(res_column), element_type, dimension);
    });
}

INSTANTIATE_INDEX_IMPL(ColumnDenseVector)

ColumnPtr ColumnDenseVector::replicate(const Offsets & offsets) const
{
    const size_t col_size = size();
    if (col_size != offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column.");

    return dispatchByElementType(element_type, [&](auto tag) -> ColumnPtr
    {
        using T = decltype(tag);
        const auto & data = getTypedData<T>();

        auto res_nested = ColumnVector<T>::create();
        auto & res_data = res_nested->getData();

        if (col_size && offsets.back())
        {
            res_data.resize(offsets.back() * dimension);

            Offset curr_offset = 0;
            for (size_t i = 0; i < col_size; ++i)
                for (size_t next_offset = offsets[i]; curr_offset < next_offset; ++curr_offset)
                    memcpy(&res_data[curr_offset * dimension], &data[i * dimension], dimension * sizeof(T));
        }

        MutableColumnPtr res_column = std::move(res_nested);
        return ColumnDenseVector::create(std::move(res_column), element_type, dimension);
    });
}

ColumnPtr ColumnDenseVector::compress(bool force_compression) const
{
    auto compressed = nested->compress(force_compression);
    const auto byte_size = compressed->byteSize();
    return ColumnCompressed::create(
        size(),
        byte_size,
        [my_compressed = std::move(compressed), my_element_type = element_type, my_dimension = dimension]
        { return ColumnDenseVector::create(my_compressed->decompress(), my_element_type, my_dimension); });
}

void ColumnDenseVector::getExtremes(Field & min, Field & max, size_t, size_t) const
{
    /// Min/max are ill-defined for vectors; return the default value, like ColumnArray does.
    Array default_value(dimension, Field(Float64(0)));
    min = default_value;
    max = std::move(default_value);
}

struct ColumnDenseVector::ComparatorBase
{
    const ColumnDenseVector & parent;
    int nan_direction_hint;

    ComparatorBase(const ColumnDenseVector & parent_, int nan_direction_hint_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        return parent.compareAtImpl(lhs, rhs, parent, nan_direction_hint);
    }
};

void ColumnDenseVector::getPermutation(
    PermutationSortDirection direction, PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
}

void ColumnDenseVector::updatePermutation(
    PermutationSortDirection direction,
    PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this, nan_direction_hint);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(
            limit, res, equal_ranges, ComparatorAscendingUnstable(*this, nan_direction_hint), comparator_equal,
            DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(
            limit, res, equal_ranges, ComparatorAscendingStable(*this, nan_direction_hint), comparator_equal,
            DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(
            limit, res, equal_ranges, ComparatorDescendingUnstable(*this, nan_direction_hint), comparator_equal,
            DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(
            limit, res, equal_ranges, ComparatorDescendingStable(*this, nan_direction_hint), comparator_equal,
            DefaultSort(), DefaultPartialSort());
}

void ColumnDenseVector::forEachMutableSubcolumn(MutableColumnCallback callback)
{
    callback(nested);
}

void ColumnDenseVector::forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*nested);
    nested->forEachMutableSubcolumnRecursively(callback);
}

void ColumnDenseVector::forEachSubcolumn(ColumnCallback callback) const
{
    callback(nested);
}

void ColumnDenseVector::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    callback(*nested);
    nested->forEachSubcolumnRecursively(callback);
}

}
