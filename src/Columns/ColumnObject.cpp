#include <Core/Field.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
    extern const int NUMBER_OF_DIMENSIONS_MISMATHED;
    extern const int NOT_IMPLEMENTED;
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
        return num_dimensions
            ? createEmptyArrayField(num_dimensions)
            : replacement;
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

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t>
{
public:
    size_t operator()(const Array & x) const
    {
        const size_t size = x.size();
        std::optional<size_t> dimensions;

        for (size_t i = 0; i < size; ++i)
        {
            /// Do not count Nulls, because they will be replaced by default
            /// values with proper number of dimensions.
            if (x[i].isNull())
                continue;

            size_t current_dimensions = applyVisitor(*this, x[i]);
            if (!dimensions)
                dimensions = current_dimensions;
            else if (current_dimensions != *dimensions)
                throw Exception(ErrorCodes::NUMBER_OF_DIMENSIONS_MISMATHED,
                    "Number of dimensions mismatched among array elements");
        }

        return 1 + dimensions.value_or(0);
    }

    template <typename T>
    size_t operator()(const T &) const { return 0; }
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

    DataTypePtr getScalarType() const { return getLeastSupertype(type_indexes, true); }
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

    return
    {
        to_scalar_type_visitor.getScalarType(),
        to_scalar_type_visitor.haveNulls(),
        to_scalar_type_visitor.needConvertField(),
        applyVisitor(FieldVisitorToNumberOfDimensions(), field),
    };
}

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr && data_, bool is_nullable_)
    : least_common_type(getDataTypeByColumn(*data_))
    , is_nullable(is_nullable_)
{
    data.push_back(std::move(data_));
}

ColumnObject::Subcolumn::Subcolumn(
    size_t size_, bool is_nullable_)
    : least_common_type(std::make_shared<DataTypeNothing>())
    , is_nullable(is_nullable_)
    , num_of_defaults_in_prefix(size_)
{
}

size_t ColumnObject::Subcolumn::Subcolumn::size() const
{
    size_t res = num_of_defaults_in_prefix;
    for (const auto & part : data)
        res += part->size();
    return res;
}

size_t ColumnObject::Subcolumn::Subcolumn::byteSize() const
{
    size_t res = 0;
    for (const auto & part : data)
        res += part->byteSize();
    return res;
}

size_t ColumnObject::Subcolumn::Subcolumn::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & part : data)
        res += part->allocatedBytes();
    return res;
}

void ColumnObject::Subcolumn::checkTypes() const
{
    DataTypes prefix_types;
    prefix_types.reserve(data.size());
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto current_type = getDataTypeByColumn(*data[i]);
        prefix_types.push_back(current_type);
        auto prefix_common_type = getLeastSupertype(prefix_types);
        if (!prefix_common_type->equals(*current_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Data type {} of column at position {} cannot represent all columns from i-th prefix",
                current_type->getName(), i);
    }
}

void ColumnObject::Subcolumn::insert(Field field)
{
    auto info = getFieldInfo(field);
    insert(std::move(field), std::move(info));
}

void ColumnObject::Subcolumn::addNewColumnPart(DataTypePtr type)
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

    return (is_native_int || is_native_uint)
        && lhs.getSizeOfValueInMemory() <= rhs.getSizeOfValueInMemory();
}

void ColumnObject::Subcolumn::insert(Field field, FieldInfo info)
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

    if (field.isNull())
        value_dim = column_dim;

    if (value_dim != column_dim)
        throw Exception(ErrorCodes::NUMBER_OF_DIMENSIONS_MISMATHED,
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
        if (!isConversionRequiredBetweenIntegers(*base_type, *least_common_base_type))
        {
            base_type = getLeastSupertype(DataTypes{std::move(base_type), least_common_base_type}, true);
            type_changed = true;
            if (!least_common_base_type->equals(*base_type))
                addNewColumnPart(createArrayOfType(std::move(base_type), value_dim));
        }
    }

    if (type_changed || info.need_convert)
        field = convertFieldToTypeOrThrow(field, *least_common_type.get());

    data.back()->insert(field);
}

void ColumnObject::Subcolumn::insertRangeFrom(const Subcolumn & src, size_t start, size_t length)
{
    assert(src.isFinalized());

    const auto & src_column = src.data.back();
    const auto & src_type = src.least_common_type.get();

    if (data.empty())
    {
        addNewColumnPart(src.least_common_type.get());
        data.back()->insertRangeFrom(*src_column, start, length);
    }
    else if (least_common_type.get()->equals(*src_type))
    {
        data.back()->insertRangeFrom(*src_column, start, length);
    }
    else
    {
        auto new_least_common_type = getLeastSupertype(DataTypes{least_common_type.get(), src_type}, true);
        auto casted_column = castColumn({src_column, src_type, ""}, new_least_common_type);

        if (!least_common_type.get()->equals(*new_least_common_type))
            addNewColumnPart(std::move(new_least_common_type));

        data.back()->insertRangeFrom(*casted_column, start, length);
    }
}

bool ColumnObject::Subcolumn::isFinalized() const
{
    return data.empty() ||
        (data.size() == 1 && !data[0]->isSparse() && num_of_defaults_in_prefix == 0);
}

void ColumnObject::Subcolumn::finalize()
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
                part = values->createWithOffsets(offsets_data, to_type->getDefault(), part_size, /*shift=*/ 0);
            }
        }

        result_column->insertRangeFrom(*part, 0, part_size);
    }

    data = { std::move(result_column) };
    num_of_defaults_in_prefix = 0;
}

void ColumnObject::Subcolumn::insertDefault()
{
    if (data.empty())
        ++num_of_defaults_in_prefix;
    else
        data.back()->insertDefault();
}

void ColumnObject::Subcolumn::insertManyDefaults(size_t length)
{
    if (data.empty())
        num_of_defaults_in_prefix += length;
    else
        data.back()->insertManyDefaults(length);
}

void ColumnObject::Subcolumn::popBack(size_t n)
{
    assert(n <= size());

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

Field ColumnObject::Subcolumn::getLastField() const
{
    if (data.empty())
        return Field();

    const auto & last_part = data.back();
    assert(!last_part->empty());
    return (*last_part)[last_part->size() - 1];
}

ColumnObject::Subcolumn ColumnObject::Subcolumn::recreateWithDefaultValues(const FieldInfo & field_info) const
{
    auto scalar_type = field_info.scalar_type;
    if (is_nullable)
        scalar_type = makeNullable(scalar_type);

    Subcolumn new_subcolumn;
    new_subcolumn.least_common_type = LeastCommonType{createArrayOfType(scalar_type, field_info.num_dimensions)};
    new_subcolumn.is_nullable = is_nullable;
    new_subcolumn.num_of_defaults_in_prefix = num_of_defaults_in_prefix;
    new_subcolumn.data.reserve(data.size());

    for (const auto & part : data)
        new_subcolumn.data.push_back(recreateColumnWithDefaultValues(
            part, scalar_type, field_info.num_dimensions));

    return new_subcolumn;
}

IColumn & ColumnObject::Subcolumn::getFinalizedColumn()
{
    assert(isFinalized());
    return *data[0];
}

const IColumn & ColumnObject::Subcolumn::getFinalizedColumn() const
{
    assert(isFinalized());
    return *data[0];
}

const ColumnPtr & ColumnObject::Subcolumn::getFinalizedColumnPtr() const
{
    assert(isFinalized());
    return data[0];
}

ColumnObject::Subcolumn::LeastCommonType::LeastCommonType(DataTypePtr type_)
    : type(std::move(type_))
    , base_type(getBaseTypeOfArray(type))
    , num_dimensions(DB::getNumberOfDimensions(*type))
{
}

ColumnObject::ColumnObject(bool is_nullable_)
    : is_nullable(is_nullable_)
    , num_rows(0)
{
}

ColumnObject::ColumnObject(SubcolumnsTree && subcolumns_, bool is_nullable_)
    : is_nullable(is_nullable_)
    , subcolumns(std::move(subcolumns_))
    , num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->data.size())

{
    checkConsistency();
}

void ColumnObject::checkConsistency() const
{
    if (subcolumns.empty())
        return;

    for (const auto & leaf : subcolumns)
    {
        if (num_rows != leaf->data.size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of subcolumns are inconsistent in ColumnObject."
                " Subcolumn '{}' has {} rows, but expected size is {}",
                leaf->path.getPath(), leaf->data.size(), num_rows);
        }
    }
}

size_t ColumnObject::size() const
{
#ifndef NDEBUG
    checkConsistency();
#endif
    return num_rows;
}

MutableColumnPtr ColumnObject::cloneResized(size_t new_size) const
{
    /// cloneResized with new_size == 0 is used for cloneEmpty().
    if (new_size != 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ColumnObject doesn't support resize to non-zero length");

    return ColumnObject::create(is_nullable);
}

size_t ColumnObject::byteSize() const
{
    size_t res = 0;
    for (const auto & entry : subcolumns)
        res += entry->data.byteSize();
    return res;
}

size_t ColumnObject::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & entry : subcolumns)
        res += entry->data.allocatedBytes();
    return res;
}

void ColumnObject::forEachSubcolumn(ColumnCallback callback)
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot iterate over non-finalized ColumnObject");

    for (auto & entry : subcolumns)
        callback(entry->data.data.back());
}

void ColumnObject::insert(const Field & field)
{
    const auto & object = field.get<const Object &>();

    HashSet<StringRef, StringRefHash> inserted;
    size_t old_size = size();
    for (const auto & [key_str, value] : object)
    {
        PathInData key(key_str);
        inserted.insert(key_str);
        if (!hasSubcolumn(key))
            addSubcolumn(key, old_size);

        auto & subcolumn = getSubcolumn(key);
        subcolumn.insert(value);
    }

    for (auto & entry : subcolumns)
        if (!inserted.has(entry->path.getPath()))
            entry->data.insertDefault();

    ++num_rows;
}

void ColumnObject::insertDefault()
{
    for (auto & entry : subcolumns)
        entry->data.insertDefault();

    ++num_rows;
}

Field ColumnObject::operator[](size_t n) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get Field from non-finalized ColumnObject");

    Object object;
    for (const auto & entry : subcolumns)
        object[entry->path.getPath()] = (*entry->data.data.back())[n];

    return object;
}

void ColumnObject::get(size_t n, Field & res) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get Field from non-finalized ColumnObject");

    auto & object = res.get<Object &>();
    for (const auto & entry : subcolumns)
    {
        auto it = object.try_emplace(entry->path.getPath()).first;
        entry->data.data.back()->get(n, it->second);
    }
}

void ColumnObject::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & src_object = assert_cast<const ColumnObject &>(src);

    for (auto & entry : subcolumns)
    {
        if (src_object.hasSubcolumn(entry->path))
            entry->data.insertRangeFrom(src_object.getSubcolumn(entry->path), start, length);
        else
            entry->data.insertManyDefaults(length);
    }

    num_rows += length;
    finalize();
}

ColumnPtr ColumnObject::replicate(const Offsets & offsets) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot replicate non-finalized ColumnObject");

    auto res_column = ColumnObject::create(is_nullable);
    for (const auto & entry : subcolumns)
    {
        auto replicated_data = entry->data.data.back()->replicate(offsets)->assumeMutable();
        res_column->addSubcolumn(entry->path, std::move(replicated_data));
    }

    return res_column;
}

void ColumnObject::popBack(size_t length)
{
    for (auto & entry : subcolumns)
        entry->data.popBack(length);

    num_rows -= length;
}

const ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const PathInData & key) const
{
    if (const auto * node = subcolumns.findLeaf(key))
        return node->data;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key.getPath());
}

ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const PathInData & key)
{
    if (const auto * node = subcolumns.findLeaf(key))
        return const_cast<SubcolumnsTree::Node *>(node)->data;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key.getPath());
}

bool ColumnObject::hasSubcolumn(const PathInData & key) const
{
    return subcolumns.findLeaf(key) != nullptr;
}

void ColumnObject::addSubcolumn(const PathInData & key, MutableColumnPtr && subcolumn)
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

void ColumnObject::addSubcolumn(const PathInData & key, size_t new_size)
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

void ColumnObject::addNestedSubcolumn(const PathInData & key, const FieldInfo & field_info, size_t new_size)
{
    if (!key.hasNested())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add Nested subcolumn, because path doesn't contain Nested");

    bool inserted = false;
    /// We find node that represents the same Nested type as @key.
    const auto * nested_node = subcolumns.findBestMatch(key);

    if (nested_node)
    {
        /// Find any leaf of Nested subcolumn.
        const auto * leaf = subcolumns.findLeaf(nested_node, [&](const auto &) { return true; });
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
}

PathsInData ColumnObject::getKeys() const
{
    PathsInData keys;
    keys.reserve(subcolumns.size());
    for (const auto & entry : subcolumns)
        keys.emplace_back(entry->path);
    return keys;
}

bool ColumnObject::isFinalized() const
{
    return std::all_of(subcolumns.begin(), subcolumns.end(),
        [](const auto & entry) { return entry->data.isFinalized(); });
}

void ColumnObject::finalize()
{
    size_t old_size = size();
    SubcolumnsTree new_subcolumns;
    for (auto && entry : subcolumns)
    {
        const auto & least_common_type = entry->data.getLeastCommonType();

        /// Do not add subcolumns, which consists only from NULLs.
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

}
