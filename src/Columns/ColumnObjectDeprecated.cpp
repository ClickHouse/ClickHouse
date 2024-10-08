#include <Core/Field.h>
#include <Columns/ColumnObjectDeprecated.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Common/iota.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/HashTable/HashSet.h>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int DUPLICATE_COLUMN;
    extern const int EXPERIMENTAL_FEATURE_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_DIMENSIONS_MISMATCHED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

/// Recreates column with default scalar values and keeps sizes of arrays.
ColumnPtr recreateColumnWithDefaultValues(
    const ColumnPtr & column, const DataTypePtr & scalar_type, size_t num_dimensions)
{
    const auto * column_array = checkAndGetColumn<ColumnArray>(column.get());
    if (column_array && num_dimensions)
    {
        return ColumnArray::create(
            recreateColumnWithDefaultValues(
                column_array->getDataPtr(), scalar_type, num_dimensions - 1),
                IColumn::mutate(column_array->getOffsetsPtr()));
    }

    return createArrayOfType(scalar_type, num_dimensions)->createColumn()->cloneResized(column->size());
}

/// Replaces NULL fields to given field or empty array.
class FieldVisitorReplaceNull : public StaticVisitor<Field>
{
public:
    explicit FieldVisitorReplaceNull(
        const Field & replacement_, size_t num_dimensions_)
        : replacement(replacement_)
        , num_dimensions(num_dimensions_)
    {
    }

    Field operator()(const Null &) const
    {
        return num_dimensions ? Array() : replacement;
    }

    Field operator()(const Array & x) const
    {
        assert(num_dimensions > 0);
        const size_t size = x.size();
        Array res(size);
        for (size_t i = 0; i < size; ++i)
            res[i] = applyVisitor(FieldVisitorReplaceNull(replacement, num_dimensions - 1), x[i]);
        return res;
    }

    template <typename T>
    Field operator()(const T & x) const { return x; }

private:
    const Field & replacement;
    size_t num_dimensions;
};

/// Visitor that allows to get type of scalar field
/// or least common type of scalars in array.
/// More optimized version of FieldToDataType.
class FieldVisitorToScalarType : public StaticVisitor<>
{
public:
    using FieldType = Field::Types::Which;

    void operator()(const Array & x)
    {
        size_t size = x.size();
        for (size_t i = 0; i < size; ++i)
            applyVisitor(*this, x[i]);
    }

    void operator()(const UInt64 & x)
    {
        field_types.insert(FieldType::UInt64);
        if (x <= std::numeric_limits<UInt8>::max())
            type_indexes.insert(TypeIndex::UInt8);
        else if (x <= std::numeric_limits<UInt16>::max())
            type_indexes.insert(TypeIndex::UInt16);
        else if (x <= std::numeric_limits<UInt32>::max())
            type_indexes.insert(TypeIndex::UInt32);
        else
            type_indexes.insert(TypeIndex::UInt64);
    }

    void operator()(const Int64 & x)
    {
        field_types.insert(FieldType::Int64);
        if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min())
            type_indexes.insert(TypeIndex::Int8);
        else if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min())
            type_indexes.insert(TypeIndex::Int16);
        else if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min())
            type_indexes.insert(TypeIndex::Int32);
        else
            type_indexes.insert(TypeIndex::Int64);
    }

    void operator()(const bool &)
    {
        field_types.insert(FieldType::UInt64);
        type_indexes.insert(TypeIndex::UInt8);
    }

    void operator()(const Null &)
    {
        have_nulls = true;
    }

    template <typename T>
    void operator()(const T &)
    {
        field_types.insert(Field::TypeToEnum<NearestFieldType<T>>::value);
        type_indexes.insert(TypeToTypeIndex<NearestFieldType<T>>);
    }

    DataTypePtr getScalarType() const { return getLeastSupertypeOrString(type_indexes); }
    bool haveNulls() const { return have_nulls; }
    bool needConvertField() const { return field_types.size() > 1; }

private:
    TypeIndexSet type_indexes;
    std::unordered_set<FieldType> field_types;
    bool have_nulls = false;
};

}

FieldInfo getFieldInfo(const Field & field)
{
    FieldVisitorToScalarType to_scalar_type_visitor;
    applyVisitor(to_scalar_type_visitor, field);
    FieldVisitorToNumberOfDimensions to_number_dimension_visitor;

    return
    {
        to_scalar_type_visitor.getScalarType(),
        to_scalar_type_visitor.haveNulls(),
        to_scalar_type_visitor.needConvertField(),
        applyVisitor(to_number_dimension_visitor, field),
        to_number_dimension_visitor.need_fold_dimension
    };
}

ColumnObjectDeprecated::Subcolumn::Subcolumn(MutableColumnPtr && data_, bool is_nullable_)
    : least_common_type(getDataTypeByColumn(*data_))
    , is_nullable(is_nullable_)
    , num_rows(data_->size())
{
    data.push_back(std::move(data_));
}

ColumnObjectDeprecated::Subcolumn::Subcolumn(
    size_t size_, bool is_nullable_)
    : least_common_type(std::make_shared<DataTypeNothing>())
    , is_nullable(is_nullable_)
    , num_of_defaults_in_prefix(size_)
    , num_rows(size_)
{
}

size_t ColumnObjectDeprecated::Subcolumn::size() const
{
    return num_rows;
}

size_t ColumnObjectDeprecated::Subcolumn::byteSize() const
{
    size_t res = 0;
    for (const auto & part : data)
        res += part->byteSize();
    return res;
}

size_t ColumnObjectDeprecated::Subcolumn::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & part : data)
        res += part->allocatedBytes();
    return res;
}

void ColumnObjectDeprecated::Subcolumn::get(size_t n, Field & res) const
{
    if (isFinalized())
    {
        getFinalizedColumn().get(n, res);
        return;
    }

    size_t ind = n;
    if (ind < num_of_defaults_in_prefix)
    {
        res = least_common_type.get()->getDefault();
        return;
    }

    ind -= num_of_defaults_in_prefix;
    for (const auto & part : data)
    {
        if (ind < part->size())
        {
            part->get(ind, res);
            res = convertFieldToTypeOrThrow(res, *least_common_type.get());
            return;
        }

        ind -= part->size();
    }

    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Index ({}) for getting field is out of range", n);
}

void ColumnObjectDeprecated::Subcolumn::checkTypes() const
{
    DataTypes prefix_types;
    prefix_types.reserve(data.size());
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto current_type = getDataTypeByColumn(*data[i]);
        prefix_types.push_back(current_type);
        auto prefix_common_type = getLeastSupertype(prefix_types);
        if (!prefix_common_type->equals(*current_type))
            throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
                "Data type {} of column at position {} cannot represent all columns from i-th prefix",
                current_type->getName(), i);
    }
}

void ColumnObjectDeprecated::Subcolumn::insert(Field field)
{
    auto info = DB::getFieldInfo(field);
    insert(std::move(field), std::move(info));
}

void ColumnObjectDeprecated::Subcolumn::addNewColumnPart(DataTypePtr type)
{
    auto serialization = type->getSerialization(ISerialization::Kind::SPARSE);
    data.push_back(type->createColumn(*serialization));
    least_common_type = LeastCommonType{std::move(type)};
}

static bool isConversionRequiredBetweenIntegers(const IDataType & lhs, const IDataType & rhs)
{
    /// If both of types are signed/unsigned integers and size of left field type
    /// is less than right type, we don't need to convert field,
    /// because all integer fields are stored in Int64/UInt64.

    WhichDataType which_lhs(lhs);
    WhichDataType which_rhs(rhs);

    bool is_native_int = which_lhs.isNativeInt() && which_rhs.isNativeInt();
    bool is_native_uint = which_lhs.isNativeUInt() && which_rhs.isNativeUInt();

    return (!is_native_int && !is_native_uint)
        || lhs.getSizeOfValueInMemory() > rhs.getSizeOfValueInMemory();
}

void ColumnObjectDeprecated::Subcolumn::insert(Field field, FieldInfo info)
{
    auto base_type = std::move(info.scalar_type);

    if (isNothing(base_type) && info.num_dimensions == 0)
    {
        insertDefault();
        return;
    }

    auto column_dim = least_common_type.getNumberOfDimensions();
    auto value_dim = info.num_dimensions;

    if (isNothing(least_common_type.get()))
        column_dim = value_dim;

    if (isNothing(base_type))
        value_dim = column_dim;

    if (value_dim != column_dim)
        throw Exception(ErrorCodes::NUMBER_OF_DIMENSIONS_MISMATCHED,
            "Dimension of types mismatched between inserted value and column. "
            "Dimension of value: {}. Dimension of column: {}",
             value_dim, column_dim);

    if (is_nullable)
        base_type = makeNullable(base_type);

    if (!is_nullable && info.have_nulls)
        field = applyVisitor(FieldVisitorReplaceNull(base_type->getDefault(), value_dim), std::move(field));

    bool type_changed = false;
    const auto & least_common_base_type = least_common_type.getBase();

    if (data.empty())
    {
        addNewColumnPart(createArrayOfType(std::move(base_type), value_dim));
    }
    else if (!least_common_base_type->equals(*base_type) && !isNothing(base_type))
    {
        if (isConversionRequiredBetweenIntegers(*base_type, *least_common_base_type))
        {
            base_type = getLeastSupertypeOrString(DataTypes{std::move(base_type), least_common_base_type});
            type_changed = true;
            if (!least_common_base_type->equals(*base_type))
                addNewColumnPart(createArrayOfType(std::move(base_type), value_dim));
        }
    }

    if (type_changed || info.need_convert)
        field = convertFieldToTypeOrThrow(field, *least_common_type.get());

    if (!data.back()->tryInsert(field))
    {
        /** Normalization of the field above is pretty complicated (it uses several FieldVisitors),
          * so in the case of a bug, we may get mismatched types.
          * The `IColumn::insert` method does not check the type of the inserted field, and it can lead to a segmentation fault.
          * Therefore, we use the safer `tryInsert` method to get an exception instead of a segmentation fault.
          */
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Cannot insert field {} to column {}",
            field.dump(), data.back()->dumpStructure());
    }

    ++num_rows;
}

void ColumnObjectDeprecated::Subcolumn::insertRangeFrom(const Subcolumn & src, size_t start, size_t length)
{
    assert(start + length <= src.size());
    size_t end = start + length;
    num_rows += length;

    if (data.empty())
    {
        addNewColumnPart(src.getLeastCommonType());
    }
    else if (!least_common_type.get()->equals(*src.getLeastCommonType()))
    {
        auto new_least_common_type = getLeastSupertypeOrString(DataTypes{least_common_type.get(), src.getLeastCommonType()});
        if (!new_least_common_type->equals(*least_common_type.get()))
            addNewColumnPart(std::move(new_least_common_type));
    }

    if (end <= src.num_of_defaults_in_prefix)
    {
        data.back()->insertManyDefaults(length);
        return;
    }

    if (start < src.num_of_defaults_in_prefix)
        data.back()->insertManyDefaults(src.num_of_defaults_in_prefix - start);

    auto insert_from_part = [&](const auto & column, size_t from, size_t n)
    {
        assert(from + n <= column->size());
        auto column_type = getDataTypeByColumn(*column);

        if (column_type->equals(*least_common_type.get()))
        {
            data.back()->insertRangeFrom(*column, from, n);
            return;
        }

        /// If we need to insert large range, there is no sense to cut part of column and cast it.
        /// Casting of all column and inserting from it can be faster.
        /// Threshold is just a guess.

        if (n * 3 >= column->size())
        {
            auto casted_column = castColumn({column, column_type, ""}, least_common_type.get());
            data.back()->insertRangeFrom(*casted_column, from, n);
            return;
        }

        auto casted_column = column->cut(from, n);
        casted_column = castColumn({casted_column, column_type, ""}, least_common_type.get());
        data.back()->insertRangeFrom(*casted_column, 0, n);
    };

    size_t pos = 0;
    size_t processed_rows = src.num_of_defaults_in_prefix;

    /// Find the first part of the column that intersects the range.
    while (pos < src.data.size() && processed_rows + src.data[pos]->size() < start)
    {
        processed_rows += src.data[pos]->size();
        ++pos;
    }

    /// Insert from the first part of column.
    if (pos < src.data.size() && processed_rows < start)
    {
        size_t part_start = start - processed_rows;
        size_t part_length = std::min(src.data[pos]->size() - part_start, end - start);
        insert_from_part(src.data[pos], part_start, part_length);
        processed_rows += src.data[pos]->size();
        ++pos;
    }

    /// Insert from the parts of column in the middle of range.
    while (pos < src.data.size() && processed_rows + src.data[pos]->size() < end)
    {
        insert_from_part(src.data[pos], 0, src.data[pos]->size());
        processed_rows += src.data[pos]->size();
        ++pos;
    }

    /// Insert from the last part of column if needed.
    if (pos < src.data.size() && processed_rows < end)
    {
        size_t part_end = end - processed_rows;
        insert_from_part(src.data[pos], 0, part_end);
    }
}

bool ColumnObjectDeprecated::Subcolumn::isFinalized() const
{
    return num_of_defaults_in_prefix == 0 &&
        (data.empty() || (data.size() == 1 && !data[0]->isSparse()));
}

void ColumnObjectDeprecated::Subcolumn::finalize()
{
    if (isFinalized())
        return;

    if (data.size() == 1 && num_of_defaults_in_prefix == 0)
    {
        data[0] = data[0]->convertToFullColumnIfSparse();
        return;
    }

    const auto & to_type = least_common_type.get();
    auto result_column = to_type->createColumn();

    if (num_of_defaults_in_prefix)
        result_column->insertManyDefaults(num_of_defaults_in_prefix);

    for (auto & part : data)
    {
        part = part->convertToFullColumnIfSparse();
        auto from_type = getDataTypeByColumn(*part);
        size_t part_size = part->size();

        if (!from_type->equals(*to_type))
        {
            auto offsets = ColumnUInt64::create();
            auto & offsets_data = offsets->getData();

            /// We need to convert only non-default values and then recreate column
            /// with default value of new type, because default values (which represents misses in data)
            /// may be inconsistent between types (e.g "0" in UInt64 and empty string in String).

            part->getIndicesOfNonDefaultRows(offsets_data, 0, part_size);

            if (offsets->size() == part_size)
            {
                part = castColumn({part, from_type, ""}, to_type);
            }
            else
            {
                auto values = part->index(*offsets, offsets->size());
                values = castColumn({values, from_type, ""}, to_type);
                part = values->createWithOffsets(offsets_data, *createColumnConstWithDefaultValue(result_column->getPtr()), part_size, /*shift=*/ 0);
            }
        }

        result_column->insertRangeFrom(*part, 0, part_size);
    }

    data = { std::move(result_column) };
    num_of_defaults_in_prefix = 0;
}

void ColumnObjectDeprecated::Subcolumn::insertDefault()
{
    if (data.empty())
        ++num_of_defaults_in_prefix;
    else
        data.back()->insertDefault();

    ++num_rows;
}

void ColumnObjectDeprecated::Subcolumn::insertManyDefaults(size_t length)
{
    if (data.empty())
        num_of_defaults_in_prefix += length;
    else
        data.back()->insertManyDefaults(length);

    num_rows += length;
}

void ColumnObjectDeprecated::Subcolumn::popBack(size_t n)
{
    assert(n <= size());

    num_rows -= n;
    size_t num_removed = 0;
    for (auto it = data.rbegin(); it != data.rend(); ++it)
    {
        if (n == 0)
            break;

        auto & column = *it;
        if (n < column->size())
        {
            column->popBack(n);
            n = 0;
        }
        else
        {
            ++num_removed;
            n -= column->size();
        }
    }

    data.resize(data.size() - num_removed);
    num_of_defaults_in_prefix -= n;
}

ColumnObjectDeprecated::Subcolumn ColumnObjectDeprecated::Subcolumn::cut(size_t start, size_t length) const
{
    Subcolumn new_subcolumn(0, is_nullable);
    new_subcolumn.insertRangeFrom(*this, start, length);
    return new_subcolumn;
}

Field ColumnObjectDeprecated::Subcolumn::getLastField() const
{
    if (data.empty())
        return Field();

    const auto & last_part = data.back();
    assert(!last_part->empty());
    return (*last_part)[last_part->size() - 1];
}

FieldInfo ColumnObjectDeprecated::Subcolumn::getFieldInfo() const
{
    const auto & base_type = least_common_type.getBase();
    return FieldInfo
    {
        .scalar_type = base_type,
        .have_nulls = base_type->isNullable(),
        .need_convert = false,
        .num_dimensions = least_common_type.getNumberOfDimensions(),
        .need_fold_dimension = false,
    };
}

ColumnObjectDeprecated::Subcolumn ColumnObjectDeprecated::Subcolumn::recreateWithDefaultValues(const FieldInfo & field_info) const
{
    auto scalar_type = field_info.scalar_type;
    if (is_nullable)
        scalar_type = makeNullable(scalar_type);

    Subcolumn new_subcolumn(*this);
    new_subcolumn.least_common_type = LeastCommonType{createArrayOfType(scalar_type, field_info.num_dimensions)};

    for (auto & part : new_subcolumn.data)
        part = recreateColumnWithDefaultValues(part, scalar_type, field_info.num_dimensions);

    return new_subcolumn;
}

IColumn & ColumnObjectDeprecated::Subcolumn::getFinalizedColumn()
{
    assert(isFinalized());
    return *data[0];
}

const IColumn & ColumnObjectDeprecated::Subcolumn::getFinalizedColumn() const
{
    assert(isFinalized());
    return *data[0];
}

const ColumnPtr & ColumnObjectDeprecated::Subcolumn::getFinalizedColumnPtr() const
{
    assert(isFinalized());
    return data[0];
}

ColumnObjectDeprecated::Subcolumn::LeastCommonType::LeastCommonType()
    : type(std::make_shared<DataTypeNothing>())
    , base_type(type)
    , num_dimensions(0)
{
}

ColumnObjectDeprecated::Subcolumn::LeastCommonType::LeastCommonType(DataTypePtr type_)
    : type(std::move(type_))
    , base_type(getBaseTypeOfArray(type))
    , num_dimensions(DB::getNumberOfDimensions(*type))
{
}

ColumnObjectDeprecated::ColumnObjectDeprecated(bool is_nullable_)
    : is_nullable(is_nullable_)
    , num_rows(0)
{
}

ColumnObjectDeprecated::ColumnObjectDeprecated(Subcolumns && subcolumns_, bool is_nullable_)
    : is_nullable(is_nullable_)
    , subcolumns(std::move(subcolumns_))
    , num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->data.size())

{
    checkConsistency();
}

void ColumnObjectDeprecated::checkConsistency() const
{
    if (subcolumns.empty())
        return;

    for (const auto & leaf : subcolumns)
    {
        if (num_rows != leaf->data.size())
        {
            throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR, "Sizes of subcolumns are inconsistent in ColumnObjectDeprecated."
                " Subcolumn '{}' has {} rows, but expected size is {}",
                leaf->path.getPath(), leaf->data.size(), num_rows);
        }
    }
}

size_t ColumnObjectDeprecated::size() const
{
#ifndef NDEBUG
    checkConsistency();
#endif
    return num_rows;
}

size_t ColumnObjectDeprecated::byteSize() const
{
    size_t res = 0;
    for (const auto & entry : subcolumns)
        res += entry->data.byteSize();
    return res;
}

size_t ColumnObjectDeprecated::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & entry : subcolumns)
        res += entry->data.allocatedBytes();
    return res;
}

void ColumnObjectDeprecated::forEachSubcolumn(MutableColumnCallback callback)
{
    for (auto & entry : subcolumns)
        for (auto & part : entry->data.data)
            callback(part);
}

void ColumnObjectDeprecated::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    for (auto & entry : subcolumns)
    {
        for (auto & part : entry->data.data)
        {
            callback(*part);
            part->forEachSubcolumnRecursively(callback);
        }
    }
}

void ColumnObjectDeprecated::insert(const Field & field)
{
    const auto & object = field.safeGet<const Object &>();

    HashSet<StringRef, StringRefHash> inserted_paths;
    size_t old_size = size();
    for (const auto & [key_str, value] : object)
    {
        PathInData key(key_str);
        inserted_paths.insert(key_str);
        if (!hasSubcolumn(key))
            addSubcolumn(key, old_size);

        auto & subcolumn = getSubcolumn(key);
        subcolumn.insert(value);
    }

    for (auto & entry : subcolumns)
    {
        if (!inserted_paths.has(entry->path.getPath()))
        {
            bool inserted = tryInsertDefaultFromNested(entry);
            if (!inserted)
                entry->data.insertDefault();
        }
    }

    ++num_rows;
}

bool ColumnObjectDeprecated::tryInsert(const Field & field)
{
    if (field.getType() != Field::Types::Which::Object)
        return false;

    insert(field);
    return true;
}

void ColumnObjectDeprecated::insertDefault()
{
    for (auto & entry : subcolumns)
        entry->data.insertDefault();

    ++num_rows;
}

Field ColumnObjectDeprecated::operator[](size_t n) const
{
    Field object;
    get(n, object);
    return object;
}

void ColumnObjectDeprecated::get(size_t n, Field & res) const
{
    assert(n < size());
    res = Object();
    auto & object = res.safeGet<Object &>();

    for (const auto & entry : subcolumns)
    {
        auto it = object.try_emplace(entry->path.getPath()).first;
        entry->data.get(n, it->second);
    }
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnObjectDeprecated::insertFrom(const IColumn & src, size_t n)
#else
void ColumnObjectDeprecated::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    insert(src[n]);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnObjectDeprecated::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnObjectDeprecated::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    const auto & src_object = assert_cast<const ColumnObjectDeprecated &>(src);

    for (const auto & entry : src_object.subcolumns)
    {
        if (!hasSubcolumn(entry->path))
        {
            if (entry->path.hasNested())
                addNestedSubcolumn(entry->path, entry->data.getFieldInfo(), num_rows);
            else
                addSubcolumn(entry->path, num_rows);
        }

        auto & subcolumn = getSubcolumn(entry->path);
        subcolumn.insertRangeFrom(entry->data, start, length);
    }

    for (auto & entry : subcolumns)
    {
        if (!src_object.hasSubcolumn(entry->path))
        {
            bool inserted = tryInsertManyDefaultsFromNested(entry);
            if (!inserted)
                entry->data.insertManyDefaults(length);
        }
    }

    num_rows += length;
    finalize();
}

void ColumnObjectDeprecated::popBack(size_t length)
{
    for (auto & entry : subcolumns)
        entry->data.popBack(length);

    num_rows -= length;
}

template <typename Func>
MutableColumnPtr ColumnObjectDeprecated::applyForSubcolumns(Func && func) const
{
    if (!isFinalized())
    {
        auto finalized = cloneFinalized();
        auto & finalized_object = assert_cast<ColumnObjectDeprecated &>(*finalized);
        return finalized_object.applyForSubcolumns(std::forward<Func>(func));
    }

    auto res = ColumnObjectDeprecated::create(is_nullable);
    for (const auto & subcolumn : subcolumns)
    {
        auto new_subcolumn = func(subcolumn->data.getFinalizedColumn());
        res->addSubcolumn(subcolumn->path, new_subcolumn->assumeMutable());
    }

    return res;
}

ColumnPtr ColumnObjectDeprecated::permute(const Permutation & perm, size_t limit) const
{
    return applyForSubcolumns([&](const auto & subcolumn) { return subcolumn.permute(perm, limit); });
}

ColumnPtr ColumnObjectDeprecated::filter(const Filter & filter, ssize_t result_size_hint) const
{
    return applyForSubcolumns([&](const auto & subcolumn) { return subcolumn.filter(filter, result_size_hint); });
}

ColumnPtr ColumnObjectDeprecated::index(const IColumn & indexes, size_t limit) const
{
    return applyForSubcolumns([&](const auto & subcolumn) { return subcolumn.index(indexes, limit); });
}

ColumnPtr ColumnObjectDeprecated::replicate(const Offsets & offsets) const
{
    return applyForSubcolumns([&](const auto & subcolumn) { return subcolumn.replicate(offsets); });
}

MutableColumnPtr ColumnObjectDeprecated::cloneResized(size_t new_size) const
{
    if (new_size == 0)
        return ColumnObjectDeprecated::create(is_nullable);

    return applyForSubcolumns([&](const auto & subcolumn) { return subcolumn.cloneResized(new_size); });
}

void ColumnObjectDeprecated::getPermutation(PermutationSortDirection, PermutationSortStability, size_t, int, Permutation & res) const
{
    res.resize(num_rows);
    iota(res.data(), res.size(), size_t(0));
}

void ColumnObjectDeprecated::getExtremes(Field & min, Field & max) const
{
    if (num_rows == 0)
    {
        min = Object();
        max = Object();
    }
    else
    {
        get(0, min);
        get(0, max);
    }
}

const ColumnObjectDeprecated::Subcolumn & ColumnObjectDeprecated::getSubcolumn(const PathInData & key) const
{
    if (const auto * node = subcolumns.findLeaf(key))
        return node->data;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObjectDeprecated", key.getPath());
}

ColumnObjectDeprecated::Subcolumn & ColumnObjectDeprecated::getSubcolumn(const PathInData & key)
{
    if (const auto * node = subcolumns.findLeaf(key))
        return const_cast<Subcolumns::Node *>(node)->data;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObjectDeprecated", key.getPath());
}

bool ColumnObjectDeprecated::hasSubcolumn(const PathInData & key) const
{
    return subcolumns.findLeaf(key) != nullptr;
}

void ColumnObjectDeprecated::addSubcolumn(const PathInData & key, MutableColumnPtr && subcolumn)
{
    size_t new_size = subcolumn->size();
    bool inserted = subcolumns.add(key, Subcolumn(std::move(subcolumn), is_nullable));

    if (!inserted)
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key.getPath());

    if (num_rows == 0)
        num_rows = new_size;
    else if (new_size != num_rows)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Size of subcolumn {} ({}) is inconsistent with column size ({})",
            key.getPath(), new_size, num_rows);
}

void ColumnObjectDeprecated::addSubcolumn(const PathInData & key, size_t new_size)
{
    bool inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));
    if (!inserted)
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key.getPath());

    if (num_rows == 0)
        num_rows = new_size;
    else if (new_size != num_rows)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Required size of subcolumn {} ({}) is inconsistent with column size ({})",
            key.getPath(), new_size, num_rows);
}

void ColumnObjectDeprecated::addNestedSubcolumn(const PathInData & key, const FieldInfo & field_info, size_t new_size)
{
    if (!key.hasNested())
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Cannot add Nested subcolumn, because path doesn't contain Nested");

    bool inserted = false;
    /// We find node that represents the same Nested type as @key.
    const auto * nested_node = subcolumns.findBestMatch(key);

    if (nested_node)
    {
        /// Find any leaf of Nested subcolumn.
        const auto * leaf = Subcolumns::findLeaf(nested_node, [&](const auto &) { return true; });
        assert(leaf);

        /// Recreate subcolumn with default values and the same sizes of arrays.
        auto new_subcolumn = leaf->data.recreateWithDefaultValues(field_info);

        /// It's possible that we have already inserted value from current row
        /// to this subcolumn. So, adjust size to expected.
        if (new_subcolumn.size() > new_size)
            new_subcolumn.popBack(new_subcolumn.size() - new_size);

        assert(new_subcolumn.size() == new_size);
        inserted = subcolumns.add(key, new_subcolumn);
    }
    else
    {
        /// If node was not found just add subcolumn with empty arrays.
        inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));
    }

    if (!inserted)
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key.getPath());

    if (num_rows == 0)
        num_rows = new_size;
    else if (new_size != num_rows)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Required size of subcolumn {} ({}) is inconsistent with column size ({})",
            key.getPath(), new_size, num_rows);
}

const ColumnObjectDeprecated::Subcolumns::Node * ColumnObjectDeprecated::getLeafOfTheSameNested(const Subcolumns::NodePtr & entry) const
{
    if (!entry->path.hasNested())
        return nullptr;

    size_t old_size = entry->data.size();
    const auto * current_node = subcolumns.findLeaf(entry->path);
    const Subcolumns::Node * leaf = nullptr;

    while (current_node)
    {
        /// Try to find the first Nested up to the current node.
        const auto * node_nested = Subcolumns::findParent(current_node,
            [](const auto & candidate) { return candidate.isNested(); });

        if (!node_nested)
            break;

        /// Find the leaf with subcolumn that contains values
        /// for the last rows.
        /// If there are no leaves, skip current node and find
        /// the next node up to the current.
        leaf = Subcolumns::findLeaf(node_nested,
            [&](const auto & candidate)
            {
                return candidate.data.size() > old_size;
            });

        if (leaf)
            break;

        current_node = node_nested->parent;
    }

    if (leaf && isNothing(leaf->data.getLeastCommonTypeBase()))
        return nullptr;

    return leaf;
}

bool ColumnObjectDeprecated::tryInsertManyDefaultsFromNested(const Subcolumns::NodePtr & entry) const
{
    const auto * leaf = getLeafOfTheSameNested(entry);
    if (!leaf)
        return false;

    size_t old_size = entry->data.size();
    auto field_info = entry->data.getFieldInfo();

    /// Cut the needed range from the found leaf
    /// and replace scalar values to the correct
    /// default values for given entry.
    auto new_subcolumn = leaf->data
        .cut(old_size, leaf->data.size() - old_size)
        .recreateWithDefaultValues(field_info);

    entry->data.insertRangeFrom(new_subcolumn, 0, new_subcolumn.size());
    return true;
}

bool ColumnObjectDeprecated::tryInsertDefaultFromNested(const Subcolumns::NodePtr & entry) const
{
    const auto * leaf = getLeafOfTheSameNested(entry);
    if (!leaf)
        return false;

    auto last_field = leaf->data.getLastField();
    if (last_field.isNull())
        return false;

    size_t leaf_num_dimensions = leaf->data.getNumberOfDimensions();
    size_t entry_num_dimensions = entry->data.getNumberOfDimensions();

    auto default_scalar = entry_num_dimensions > leaf_num_dimensions
        ? createEmptyArrayField(entry_num_dimensions - leaf_num_dimensions)
        : entry->data.getLeastCommonTypeBase()->getDefault();

    auto default_field = applyVisitor(FieldVisitorReplaceScalars(default_scalar, leaf_num_dimensions), last_field);
    entry->data.insert(std::move(default_field));
    return true;
}

PathsInData ColumnObjectDeprecated::getKeys() const
{
    PathsInData keys;
    keys.reserve(subcolumns.size());
    for (const auto & entry : subcolumns)
        keys.emplace_back(entry->path);
    return keys;
}

bool ColumnObjectDeprecated::isFinalized() const
{
    return std::all_of(subcolumns.begin(), subcolumns.end(),
        [](const auto & entry) { return entry->data.isFinalized(); });
}

void ColumnObjectDeprecated::finalize()
{
    size_t old_size = size();
    Subcolumns new_subcolumns;
    for (auto && entry : subcolumns)
    {
        const auto & least_common_type = entry->data.getLeastCommonType();

        /// Do not add subcolumns, which consist only from NULLs.
        if (isNothing(getBaseTypeOfArray(least_common_type)))
            continue;

        entry->data.finalize();
        new_subcolumns.add(entry->path, entry->data);
    }

    /// If all subcolumns were skipped add a dummy subcolumn,
    /// because Tuple type must have at least one element.
    if (new_subcolumns.empty())
        new_subcolumns.add(PathInData{COLUMN_NAME_DUMMY}, Subcolumn{ColumnUInt8::create(old_size, 0), is_nullable});

    std::swap(subcolumns, new_subcolumns);
    checkObjectHasNoAmbiguosPaths(getKeys());
}

void ColumnObjectDeprecated::updateHashFast(SipHash & hash) const
{
    for (const auto & entry : subcolumns)
        for (auto & part : entry->data.data)
            part->updateHashFast(hash);
}

}
