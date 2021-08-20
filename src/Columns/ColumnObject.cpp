#include <Core/Field.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
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
}

namespace
{

class FieldVisitorReplaceNull : public StaticVisitor<Field>
{
public:
    [[maybe_unused]] explicit FieldVisitorReplaceNull(const Field & replacement_)
        : replacement(replacement_)
    {
    }

    Field operator()(const Null &) const { return replacement; }

    template <typename T>
    Field operator()(const T & x) const
    {
        if constexpr (std::is_base_of_v<FieldVector, T>)
        {
            const size_t size = x.size();
            T res(size);
            for (size_t i = 0; i < size; ++i)
                res[i] = applyVisitor(*this, x[i]);
            return res;
        }
        else
            return x;
    }

private:
    Field replacement;
};

class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t>
{
public:
    size_t operator()(const Array & x) const
    {
        if (x.empty())
            return 1;

        const size_t size = x.size();
        size_t dimensions = applyVisitor(*this, x[0]);
        for (size_t i = 1; i < size; ++i)
        {
            size_t current_dimensions = applyVisitor(*this, x[i]);
            if (current_dimensions != dimensions)
                throw Exception(ErrorCodes::NUMBER_OF_DIMENSIONS_MISMATHED,
                    "Number of dimensions mismatched among array elements");
        }

        return 1 + dimensions;
    }

    template <typename T>
    size_t operator()(const T &) const { return 0; }
};

class FieldVisitorToScalarType: public StaticVisitor<>
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

    DataTypePtr getScalarType() const
    {

        auto res = getLeastSupertype(type_indexes, true);
        if (have_nulls)
            return makeNullable(res);

        return res;
    }

    bool needConvertField() const { return field_types.size() > 1; }

private:
    TypeIndexSet type_indexes;
    std::unordered_set<FieldType> field_types;
    bool have_nulls = false;
};

}

ColumnObject::Subcolumn::Subcolumn(const Subcolumn & other)
    : least_common_type(other.least_common_type)
    , is_nullable(other.is_nullable)
    , data(other.data)
    , num_of_defaults_in_prefix(other.num_of_defaults_in_prefix)
{
}

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr && data_)
    : least_common_type(getDataTypeByColumn(*data_))
    , is_nullable(least_common_type->isNullable())
{
    data.push_back(std::move(data_));
}

ColumnObject::Subcolumn::Subcolumn(size_t size_, bool is_nullable_)
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
                "Data type {} of column at postion {} cannot represent all columns from i-th prefix",
                current_type->getName(), i);
    }
}

void ColumnObject::Subcolumn::insert(Field field)
{
    auto value_dim = applyVisitor(FieldVisitorToNumberOfDimensions(), field);
    auto column_dim = getNumberOfDimensions(*least_common_type);

    if (!isNothing(least_common_type) && value_dim != column_dim)
        throw Exception(ErrorCodes::NUMBER_OF_DIMENSIONS_MISMATHED,
            "Dimension of types mismatched beetwen inserted value and column."
            "Dimension of value: {}. Dimension of column: {}",
             value_dim, column_dim);

    FieldVisitorToScalarType to_scalar_type_visitor;
    applyVisitor(to_scalar_type_visitor, field);

    auto base_type = to_scalar_type_visitor.getScalarType();
    if (isNothing(base_type))
    {
        insertDefault();
        return;
    }

    if (is_nullable && !base_type->isNullable())
        base_type = makeNullable(base_type);

    if (!is_nullable && base_type->isNullable())
    {
        base_type = removeNullable(base_type);
        if (isNothing(base_type))
        {
            insertDefault();
            return;
        }

        field = applyVisitor(FieldVisitorReplaceNull(base_type->getDefault()), std::move(field));
    }

    auto value_type = createArrayOfType(base_type, value_dim);
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

    if (type_changed || to_scalar_type_visitor.needConvertField())
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
{
}

ColumnObject::ColumnObject(SubcolumnsMap && subcolumns_, bool is_nullable_)
    : subcolumns(std::move(subcolumns_))
    , is_nullable(is_nullable_)
{
    checkConsistency();
}

void ColumnObject::checkConsistency() const
{
    if (subcolumns.empty())
        return;

    size_t first_size = subcolumns.begin()->second.size();
    for (const auto & [name, column] : subcolumns)
    {
        if (first_size != column.size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of subcolumns are inconsistent in ColumnObject."
                " Subcolumn '{}' has {} rows, subcolumn '{}' has {} rows",
                subcolumns.begin()->first, first_size, name, column.size());
        }
    }
}

size_t ColumnObject::size() const
{
#ifndef NDEBUG
    checkConsistency();
#endif
    return subcolumns.empty() ? 0 : subcolumns.begin()->second.size();
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
    for (const auto & [_, column] : subcolumns)
        res += column.byteSize();
    return res;
}

size_t ColumnObject::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & [_, column] : subcolumns)
        res += column.allocatedBytes();
    return res;
}

void ColumnObject::forEachSubcolumn(ColumnCallback callback)
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot iterate over non-finalized ColumnObject");

    for (auto & [_, column] : subcolumns)
        callback(column.data.back());
}

void ColumnObject::insert(const Field & field)
{
    const auto & object = field.get<Object>();

    HashSet<StringRef, StringRefHash> inserted;
    size_t old_size = size();
    for (const auto & [key, value] : object)
    {
        inserted.insert(key);
        if (!hasSubcolumn(key))
            addSubcolumn(key, old_size);

        auto & subcolumn = getSubcolumn(key);
        subcolumn.insert(value);
    }

    for (auto & [key, subcolumn] : subcolumns)
        if (!inserted.has(key))
            subcolumn.insertDefault();
}

void ColumnObject::insertDefault()
{
    for (auto & [_, subcolumn] : subcolumns)
        subcolumn.insertDefault();
}

Field ColumnObject::operator[](size_t n) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get Field from non-finalized ColumnObject");

    Object object;
    for (const auto & [key, subcolumn] : subcolumns)
        object[key] = (*subcolumn.data.back())[n];

    return object;
}

void ColumnObject::get(size_t n, Field & res) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get Field from non-finalized ColumnObject");

    auto & object = res.get<Object &>();
    for (const auto & [key, subcolumn] : subcolumns)
    {
        auto it = object.try_emplace(key).first;
        subcolumn.data.back()->get(n, it->second);
    }
}

void ColumnObject::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & src_object = assert_cast<const ColumnObject &>(src);

    for (auto & [name, subcolumn] : subcolumns)
    {
        if (src_object.hasSubcolumn(name))
            subcolumn.insertRangeFrom(src_object.getSubcolumn(name), start, length);
        else
            subcolumn.insertManyDefaults(length);
    }

    finalize();
}

ColumnPtr ColumnObject::replicate(const Offsets & offsets) const
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot replicate non-finalized ColumnObject");

    auto res_column = ColumnObject::create(is_nullable);
    for (auto & [key, subcolumn] : subcolumns)
        res_column->addSubcolumn(key, Subcolumn(subcolumn.data.back()->replicate(offsets)->assumeMutable()));

    return res_column;
}

void ColumnObject::popBack(size_t length)
{
    if (!isFinalized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot popBack from non-finalized ColumnObject");

    for (auto & [_, subcolumn] : subcolumns)
        subcolumn.data.back()->popBack(length);
}

const ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const String & key) const
{
    auto it = subcolumns.find(key);
    if (it != subcolumns.end())
        return it->second;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key);
}

ColumnObject::Subcolumn & ColumnObject::getSubcolumn(const String & key)
{
    auto it = subcolumns.find(key);
    if (it != subcolumns.end())
        return it->second;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "There is no subcolumn {} in ColumnObject", key);
}

bool ColumnObject::hasSubcolumn(const String & key) const
{
    return subcolumns.count(key) != 0;
}

void ColumnObject::addSubcolumn(const String & key, size_t new_size, bool check_size)
{
    if (subcolumns.count(key))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key);

    if (check_size && new_size != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key, new_size, size());

    subcolumns[key] = Subcolumn(new_size, is_nullable);
}

void ColumnObject::addSubcolumn(const String & key, Subcolumn && subcolumn, bool check_size)
{
    if (subcolumns.count(key))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Subcolumn '{}' already exists", key);

    if (check_size && subcolumn.size() != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot add subcolumn '{}' with {} rows to ColumnObject with {} rows",
            key, subcolumn.size(), size());

    subcolumn.setNullable(is_nullable);
    subcolumns[key] = std::move(subcolumn);
}

Strings ColumnObject::getKeys() const
{
    Strings keys;
    keys.reserve(subcolumns.size());
    for (const auto & [key, _] : subcolumns)
        keys.emplace_back(key);
    return keys;
}

bool ColumnObject::isFinalized() const
{
    return std::all_of(subcolumns.begin(), subcolumns.end(),
        [](const auto & elem) { return elem.second.isFinalized(); });
}

void ColumnObject::finalize()
{
    size_t old_size = size();
    SubcolumnsMap new_subcolumns;
    for (auto && [name, subcolumn] : subcolumns)
    {
        const auto & least_common_type = subcolumn.getLeastCommonType();
        if (isNothing(getBaseTypeOfArray(least_common_type)))
            continue;

        subcolumn.finalize();
        new_subcolumns[name] = std::move(subcolumn);
    }

    if (new_subcolumns.empty())
        new_subcolumns[COLUMN_NAME_DUMMY] = Subcolumn{ColumnUInt8::create(old_size)};

    std::swap(subcolumns, new_subcolumns);
    checkObjectHasNoAmbiguosPaths(getKeys());
}

}
