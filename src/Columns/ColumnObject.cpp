#include <Core/Field.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/HashTable/HashSet.h>

#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
    extern const int NUMBER_OF_DIMENSIONS_MISMATHED;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

Array createEmptyArrayField(size_t num_dimensions)
{
    Array array;
    Array * current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i)
    {
        current_array->push_back(Array());
        current_array = &current_array->back().get<Array &>();
    }

    return array;
}

ColumnPtr recreateColumnWithDefaultValues(const ColumnPtr & column)
{
    if (const auto * column_array = checkAndGetColumn<ColumnArray>(column.get()))
        return ColumnArray::create(
            recreateColumnWithDefaultValues(column_array->getDataPtr()),
                IColumn::mutate(column_array->getOffsetsPtr()));
    else
        return column->cloneEmpty()->cloneResized(column->size());
}

class FieldVisitorReplaceNull : public StaticVisitor<Field>
{
public:
    [[maybe_unused]] explicit FieldVisitorReplaceNull(
        const Field & replacement_, size_t num_dimensions_)
        : replacement(replacement_)
        , num_dimensions(num_dimensions_)
    {
    }

    template <typename T>
    Field operator()(const T & x) const
    {
        if constexpr (std::is_same_v<T, Null>)
        {
            return num_dimensions
                ? createEmptyArrayField(num_dimensions)
                : replacement;
        }
        else if constexpr (std::is_same_v<T, Array>)
        {
            assert(num_dimensions > 0);
            const size_t size = x.size();
            Array res(size);
            for (size_t i = 0; i < size; ++i)
                res[i] = applyVisitor(FieldVisitorReplaceNull(replacement, num_dimensions - 1), x[i]);
            return res;
        }
        else
            return x;
    }

private:
    const Field & replacement;
    size_t num_dimensions;
};

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
        auto field_type = Field::TypeToEnum<NearestFieldType<T>>::value;
        field_types.insert(field_type);
        type_indexes.insert(TypeId<NearestFieldType<T>>);
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

void ColumnObject::Subcolumn::insert(Field field, FieldInfo info)
{
    auto base_type = info.scalar_type;

    if (isNothing(base_type) && (!is_nullable || !info.have_nulls))
    {
        insertDefault();
        return;
    }

    auto column_dim = getNumberOfDimensions(*least_common_type);
    auto value_dim = info.num_dimensions;

    if (isNothing(least_common_type))
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

    DataTypePtr value_type;
    if (!is_nullable && info.have_nulls)
    {
        auto default_value = base_type->getDefault();
        value_type = createArrayOfType(base_type, value_dim);
        field = applyVisitor(FieldVisitorReplaceNull(default_value, value_dim), std::move(field));
    }
    else
    {
        value_type = createArrayOfType(base_type, value_dim);
    }

    bool type_changed = false;

    if (data.empty())
    {
        data.push_back(value_type->createColumn());
        least_common_type = value_type;
    }
    else if (!least_common_type->equals(*value_type))
    {
        value_type = getLeastSupertype(DataTypes{value_type, least_common_type}, true);
        type_changed = true;
        if (!least_common_type->equals(*value_type))
        {
            data.push_back(value_type->createColumn());
            least_common_type = value_type;
        }
    }

    if (type_changed || info.need_convert)
    {
        auto converted_field = convertFieldToTypeOrThrow(std::move(field), *value_type);
        data.back()->insert(std::move(converted_field));
    }
    else
        data.back()->insert(std::move(field));
}

void ColumnObject::Subcolumn::insertRangeFrom(const Subcolumn & src, size_t start, size_t length)
{
    assert(src.isFinalized());

    const auto & src_column = src.data.back();
    const auto & src_type = src.least_common_type;

    if (data.empty())
    {
        least_common_type = src_type;
        data.push_back(src_type->createColumn());
        data.back()->insertRangeFrom(*src_column, start, length);
    }
    else if (least_common_type->equals(*src_type))
    {
        data.back()->insertRangeFrom(*src_column, start, length);
    }
    else
    {
        auto new_least_common_type = getLeastSupertype(DataTypes{least_common_type, src_type}, true);
        auto casted_column = castColumn({src_column, src_type, ""}, new_least_common_type);

        if (!least_common_type->equals(*new_least_common_type))
        {
            least_common_type = new_least_common_type;
            data.push_back(least_common_type->createColumn());
        }

        data.back()->insertRangeFrom(*casted_column, start, length);
    }
}

void ColumnObject::Subcolumn::finalize()
{
    if (isFinalized() || data.empty())
        return;

    const auto & to_type = least_common_type;
    auto result_column = to_type->createColumn();

    if (num_of_defaults_in_prefix)
        result_column->insertManyDefaults(num_of_defaults_in_prefix);

    for (auto & part : data)
    {
        auto from_type = getDataTypeByColumn(*part);
        size_t part_size = part->size();

        if (!from_type->equals(*to_type))
        {
            auto offsets = ColumnUInt64::create();
            auto & offsets_data = offsets->getData();

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
    assert(n <= size);

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
    assert(!last_part.empty());
    return (*last_part)[last_part->size() - 1];
}

ColumnObject::Subcolumn ColumnObject::Subcolumn::recreateWithDefaultValues() const
{
    Subcolumn new_subcolumn;
    new_subcolumn.least_common_type = least_common_type;
    new_subcolumn.is_nullable = is_nullable;
    new_subcolumn.num_of_defaults_in_prefix = num_of_defaults_in_prefix;
    new_subcolumn.data.reserve(data.size());

    for (const auto & part : data)
        new_subcolumn.data.push_back(recreateColumnWithDefaultValues(part));

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

ColumnObject::ColumnObject(bool is_nullable_)
    : is_nullable(is_nullable_)
    , num_rows(0)
{
}

ColumnObject::ColumnObject(SubcolumnsTree && subcolumns_, bool is_nullable_)
    : is_nullable(is_nullable_)
    , subcolumns(std::move(subcolumns_))
    , num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->column.size())

{
    checkConsistency();
}

void ColumnObject::checkConsistency() const
{
    if (subcolumns.empty())
        return;

    for (const auto & leaf : subcolumns)
    {
        if (num_rows != leaf->column.size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of subcolumns are inconsistent in ColumnObject."
                " Subcolumn '{}' has {} rows, but expected size is {}",
                leaf->path.getPath(), leaf->column.size(), num_rows);
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
    if (new_size != 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ColumnObject doesn't support resize to non-zero length");

    return ColumnObject::create(is_nullable);
}

size_t ColumnObject::byteSize() const
{
    size_t res = 0;
    for (const auto & entry : subcolumns)
        res += entry->column.byteSize();
    return res;
}

size_t ColumnObject::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & entry : subcolumns)
        res += entry->column.allocatedBytes();
    return res;
}

void ColumnObject::forEachSubcolumn(ColumnCallback callback)
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot iterate over non-finalized ColumnObject");

    for (auto & entry : subcolumns)
        callback(entry->column.data.back());
}

void ColumnObject::insert(const Field & field)
{
    ++num_rows;
    const auto & object = field.get<const Object &>();

    HashSet<StringRef, StringRefHash> inserted;
    size_t old_size = size();
    for (const auto & [key_str, value] : object)
    {
        Path key(key_str);
        inserted.insert(key_str);
        if (!hasSubcolumn(key))
            addSubcolumn(key, old_size);

        auto & subcolumn = getSubcolumn(key);
        subcolumn.insert(value);
    }

    for (auto & entry : subcolumns)
        if (!inserted.has(entry->path.getPath()))
            entry->column.insertDefault();
}

void ColumnObject::insertDefault()
{
    ++num_rows;
    for (auto & entry : subcolumns)
        entry->column.insertDefault();
}

Field ColumnObject::operator[](size_t n) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get Field from non-finalized ColumnObject");

    Object object;
    for (const auto & entry : subcolumns)
        object[entry->path.getPath()] = (*entry->column.data.back())[n];

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
        entry->column.data.back()->get(n, it->second);
    }
}

void ColumnObject::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    num_rows += length;
    const auto & src_object = assert_cast<const ColumnObject &>(src);

    for (auto & entry : subcolumns)
    {
        if (src_object.hasSubcolumn(entry->path))
            entry->column.insertRangeFrom(src_object.getSubcolumn(entry->path), start, length);
        else
            entry->column.insertManyDefaults(length);
    }

    finalize();
}

ColumnPtr ColumnObject::replicate(const Offsets & offsets) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot replicate non-finalized ColumnObject");

    auto res_column = ColumnObject::create(is_nullable);
    for (const auto & entry : subcolumns)
    {
        auto replicated_data = entry->column.data.back()->replicate(offsets)->assumeMutable();
        res_column->addSubcolumn(entry->path, std::move(replicated_data), is_nullable);
    }

    return res_column;
}

void ColumnObject::popBack(size_t length)
{
    num_rows -= length;
    for (auto & entry : subcolumns)
        entry->column.popBack(length);
}

const ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const Path & key) const
{
    if (const auto * node = subcolumns.findLeaf(key))
        return node->column;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key.getPath());
}

ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const Path & key)
{
    if (const auto * node = subcolumns.findLeaf(key))
        return const_cast<SubcolumnsTree::Leaf *>(node)->column;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key.getPath());
}

bool ColumnObject::hasSubcolumn(const Path & key) const
{
    return subcolumns.findLeaf(key) != nullptr;
}

void ColumnObject::addSubcolumn(const Path & key, size_t new_size, bool check_size)
{
    if (num_rows == 0)
        num_rows = new_size;

    if (check_size && new_size != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key.getPath(), new_size, size());

    std::optional<bool> inserted;
    if (key.hasNested())
    {
        const auto * nested_node = subcolumns.findBestMatch(key);
        if (nested_node && nested_node->isNested())
        {
            const auto * leaf = subcolumns.findLeaf(nested_node, [&](const auto & ) { return true; });
            assert(leaf);

            auto new_subcolumn = leaf->column.recreateWithDefaultValues();
            if (new_subcolumn.size() > new_size)
                new_subcolumn.popBack(new_subcolumn.size() - new_size);
            else if (new_subcolumn.size() < new_size)
                new_subcolumn.insertManyDefaults(new_size - new_subcolumn.size());

            inserted = subcolumns.add(key, new_subcolumn);
        }
    }

    if (!inserted.has_value())
        inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));

    if (!*inserted)
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key.getPath());
}

void ColumnObject::addSubcolumn(const Path & key, MutableColumnPtr && subcolumn, bool check_size)
{
    if (num_rows == 0)
        num_rows = subcolumn->size();

    if (check_size && subcolumn->size() != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key.getPath(), subcolumn->size(), size());

    bool inserted = subcolumns.add(key, Subcolumn(std::move(subcolumn), is_nullable));
    if (!inserted)
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key.getPath());
}

Paths ColumnObject::getKeys() const
{
    Paths keys;
    keys.reserve(subcolumns.size());
    for (const auto & entry : subcolumns)
        keys.emplace_back(entry->path);
    return keys;
}

bool ColumnObject::isFinalized() const
{
    return std::all_of(subcolumns.begin(), subcolumns.end(),
        [](const auto & entry) { return entry->column.isFinalized(); });
}

void ColumnObject::finalize()
{
    size_t old_size = size();
    SubcolumnsTree new_subcolumns;
    for (auto && entry : subcolumns)
    {
        const auto & least_common_type = entry->column.getLeastCommonType();
        if (isNothing(getBaseTypeOfArray(least_common_type)))
            continue;

        entry->column.finalize();
        new_subcolumns.add(entry->path, std::move(entry->column));
    }

    if (new_subcolumns.empty())
        new_subcolumns.add(Path{COLUMN_NAME_DUMMY}, Subcolumn{ColumnUInt8::create(old_size), is_nullable});

    std::swap(subcolumns, new_subcolumns);
    checkObjectHasNoAmbiguosPaths(getKeys());
}

}
