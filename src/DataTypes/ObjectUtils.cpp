#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/StorageSnapshot.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int NOT_IMPLEMENTED;
}

size_t getNumberOfDimensions(const IDataType & type)
{
    if (const auto * type_array = typeid_cast<const DataTypeArray *>(&type))
        return type_array->getNumberOfDimensions();
    return 0;
}

size_t getNumberOfDimensions(const IColumn & column)
{
    if (const auto * column_array = checkAndGetColumn<ColumnArray>(column))
        return column_array->getNumberOfDimensions();
    return 0;
}

DataTypePtr getBaseTypeOfArray(const DataTypePtr & type)
{
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray * last_array = nullptr;
    const auto * current_type = type.get();
    while (const auto * type_array = typeid_cast<const DataTypeArray *>(current_type))
    {
        current_type = type_array->getNestedType().get();
        last_array = type_array;
    }

    return last_array ? last_array->getNestedType() : type;
}

ColumnPtr getBaseColumnOfArray(const ColumnPtr & column)
{
    /// Get raw pointers to avoid extra copying of column pointers.
    const ColumnArray * last_array = nullptr;
    const auto * current_column = column.get();
    while (const auto * column_array = checkAndGetColumn<ColumnArray>(current_column))
    {
        current_column = &column_array->getData();
        last_array = column_array;
    }

    return last_array ? last_array->getDataPtr() : column;
}

DataTypePtr createArrayOfType(DataTypePtr type, size_t num_dimensions)
{
    for (size_t i = 0; i < num_dimensions; ++i)
        type = std::make_shared<DataTypeArray>(std::move(type));
    return type;
}

ColumnPtr createArrayOfColumn(ColumnPtr column, size_t num_dimensions)
{
    for (size_t i = 0; i < num_dimensions; ++i)
        column = ColumnArray::create(column);
    return column;
}

Array createEmptyArrayField(size_t num_dimensions)
{
    if (num_dimensions == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create array field with 0 dimensions");

    Array array;
    Array * current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i)
    {
        current_array->push_back(Array());
        current_array = &current_array->back().get<Array &>();
    }

    return array;
}

DataTypePtr getDataTypeByColumn(const IColumn & column)
{
    auto idx = column.getDataType();
    WhichDataType which(idx);
    if (which.isSimple())
        return DataTypeFactory::instance().get(String(magic_enum::enum_name(idx)));

    if (which.isNothing())
        return std::make_shared<DataTypeNothing>();

    if (const auto * column_array = checkAndGetColumn<ColumnArray>(&column))
        return std::make_shared<DataTypeArray>(getDataTypeByColumn(column_array->getData()));

    if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(&column))
        return makeNullable(getDataTypeByColumn(column_nullable->getNestedColumn()));

    /// TODO: add more types.
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get data type of column {}", column.getFamilyName());
}

template <size_t I, typename Tuple>
static auto extractVector(const std::vector<Tuple> & vec)
{
    static_assert(I < std::tuple_size_v<Tuple>);
    std::vector<std::tuple_element_t<I, Tuple>> res;
    res.reserve(vec.size());
    for (const auto & elem : vec)
        res.emplace_back(std::get<I>(elem));
    return res;
}

static DataTypePtr recreateTupleWithElements(const DataTypeTuple & type_tuple, const DataTypes & elements)
{
    return type_tuple.haveExplicitNames()
        ? std::make_shared<DataTypeTuple>(elements, type_tuple.getElementNames())
        : std::make_shared<DataTypeTuple>(elements);
}

static std::pair<ColumnPtr, DataTypePtr> convertObjectColumnToTuple(
    const ColumnObject & column_object, const DataTypeObject & type_object)
{
    if (!column_object.isFinalized())
    {
        auto finalized = column_object.cloneFinalized();
        const auto & finalized_object = assert_cast<const ColumnObject &>(*finalized);
        return convertObjectColumnToTuple(finalized_object, type_object);
    }

    const auto & subcolumns = column_object.getSubcolumns();

    PathsInData tuple_paths;
    DataTypes tuple_types;
    Columns tuple_columns;

    for (const auto & entry : subcolumns)
    {
        tuple_paths.emplace_back(entry->path);
        tuple_types.emplace_back(entry->data.getLeastCommonType());
        tuple_columns.emplace_back(entry->data.getFinalizedColumnPtr());
    }

    return unflattenTuple(tuple_paths, tuple_types, tuple_columns);
}

static std::pair<ColumnPtr, DataTypePtr> recursivlyConvertDynamicColumnToTuple(
    const ColumnPtr & column, const DataTypePtr & type)
{
    if (!type->hasDynamicSubcolumns())
        return {column, type};

    if (const auto * type_object = typeid_cast<const DataTypeObject *>(type.get()))
    {
        const auto & column_object = assert_cast<const ColumnObject &>(*column);
        return convertObjectColumnToTuple(column_object, *type_object);
    }

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & column_array = assert_cast<const ColumnArray &>(*column);
        auto [new_column, new_type] = recursivlyConvertDynamicColumnToTuple(
            column_array.getDataPtr(), type_array->getNestedType());

        return
        {
            ColumnArray::create(new_column, column_array.getOffsetsPtr()),
            std::make_shared<DataTypeArray>(std::move(new_type)),
        };
    }

    if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & column_map = assert_cast<const ColumnMap &>(*column);
        auto [new_column, new_type] = recursivlyConvertDynamicColumnToTuple(
            column_map.getNestedColumnPtr(), type_map->getNestedType());

        return
        {
            ColumnMap::create(new_column),
            std::make_shared<DataTypeMap>(std::move(new_type)),
        };
    }

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_columns = assert_cast<const ColumnTuple &>(*column).getColumns();
        const auto & tuple_types = type_tuple->getElements();

        assert(tuple_columns.size() == tuple_types.size());
        const size_t tuple_size = tuple_types.size();

        Columns new_tuple_columns(tuple_size);
        DataTypes new_tuple_types(tuple_size);

        for (size_t i = 0; i < tuple_size; ++i)
        {
            std::tie(new_tuple_columns[i], new_tuple_types[i])
                = recursivlyConvertDynamicColumnToTuple(tuple_columns[i], tuple_types[i]);
        }

        return
        {
            ColumnTuple::create(new_tuple_columns),
            recreateTupleWithElements(*type_tuple, new_tuple_types)
        };
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Type {} unexpectedly has dynamic columns", type->getName());
}

void convertDynamicColumnsToTuples(Block & block, const StorageSnapshotPtr & storage_snapshot)
{
    for (auto & column : block)
    {
        if (!column.type->hasDynamicSubcolumns())
            continue;

        std::tie(column.column, column.type)
            = recursivlyConvertDynamicColumnToTuple(column.column, column.type);

        GetColumnsOptions options(GetColumnsOptions::AllPhysical);
        auto storage_column = storage_snapshot->tryGetColumn(options, column.name);
        if (!storage_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in storage", column.name);

        auto storage_column_concrete = storage_snapshot->getColumn(options.withExtendedObjects(), column.name);

        /// Check that constructed Tuple type and type in storage are compatible.
        getLeastCommonTypeForDynamicColumns(
            storage_column->type, {column.type, storage_column_concrete.type}, true);
    }
}

static bool isPrefix(const PathInData::Parts & prefix, const PathInData::Parts & parts)
{
    if (prefix.size() > parts.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
        if (prefix[i].key != parts[i].key)
            return false;
    return true;
}

/// Returns true if there exists a prefix with matched names,
/// but not matched structure (is Nested, number of dimensions).
static bool hasDifferentStructureInPrefix(const PathInData::Parts & lhs, const PathInData::Parts & rhs)
{
    for (size_t i = 0; i < std::min(lhs.size(), rhs.size()); ++i)
    {
        if (lhs[i].key != rhs[i].key)
            return false;
        else if (lhs[i] != rhs[i])
            return true;
    }
    return false;
}

void checkObjectHasNoAmbiguosPaths(const PathsInData & paths)
{
    size_t size = paths.size();
    for (size_t i = 0; i < size; ++i)
    {
        for (size_t j = 0; j < i; ++j)
        {
            if (isPrefix(paths[i].getParts(), paths[j].getParts())
                || isPrefix(paths[j].getParts(), paths[i].getParts()))
                throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS,
                    "Data in Object has ambiguous paths: '{}' and '{}'",
                    paths[i].getPath(), paths[j].getPath());

            if (hasDifferentStructureInPrefix(paths[i].getParts(), paths[j].getParts()))
                throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS,
                    "Data in Object has ambiguous paths: '{}' and '{}'. "
                    "Paths have prefixes matched by names, but different in structure",
                    paths[i].getPath(), paths[j].getPath());
        }
    }
}

static DataTypePtr getLeastCommonTypeForObject(const DataTypes & types, bool check_ambiguos_paths)
{
    /// Types of subcolumns by path from all tuples.
    std::unordered_map<PathInData, DataTypes, PathInData::Hash> subcolumns_types;

    /// First we flatten tuples, then get common type for paths
    /// and finally unflatten paths and create new tuple type.
    for (const auto & type : types)
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
        if (!type_tuple)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Least common type for object can be deduced only from tuples, but {} given", type->getName());

        auto [tuple_paths, tuple_types] = flattenTuple(type);
        assert(tuple_paths.size() == tuple_types.size());

        for (size_t i = 0; i < tuple_paths.size(); ++i)
            subcolumns_types[tuple_paths[i]].push_back(tuple_types[i]);
    }

    PathsInData tuple_paths;
    DataTypes tuple_types;

    /// Get the least common type for all paths.
    for (const auto & [key, subtypes] : subcolumns_types)
    {
        assert(!subtypes.empty());
        if (key.getPath() == ColumnObject::COLUMN_NAME_DUMMY)
            continue;

        size_t first_dim = getNumberOfDimensions(*subtypes[0]);
        for (size_t i = 1; i < subtypes.size(); ++i)
            if (first_dim != getNumberOfDimensions(*subtypes[i]))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Uncompatible types of subcolumn '{}': {} and {}",
                    key.getPath(), subtypes[0]->getName(), subtypes[i]->getName());

        tuple_paths.emplace_back(key);
        tuple_types.emplace_back(getLeastSupertypeOrString(subtypes));
    }

    if (tuple_paths.empty())
    {
        tuple_paths.emplace_back(ColumnObject::COLUMN_NAME_DUMMY);
        tuple_types.emplace_back(std::make_shared<DataTypeUInt8>());
    }

    if (check_ambiguos_paths)
        checkObjectHasNoAmbiguosPaths(tuple_paths);

    return unflattenTuple(tuple_paths, tuple_types);
}

static DataTypePtr getLeastCommonTypeForDynamicColumnsImpl(
    const DataTypePtr & type_in_storage, const DataTypes & concrete_types, bool check_ambiguos_paths);

template<typename Type>
static DataTypePtr getLeastCommonTypeForColumnWithNestedType(
    const Type & type, const DataTypes & concrete_types, bool check_ambiguos_paths)
{
    DataTypes nested_types;
    nested_types.reserve(concrete_types.size());

    for (const auto & concrete_type : concrete_types)
    {
        const auto * type_with_nested_conctete = typeid_cast<const Type *>(concrete_type.get());
        if (!type_with_nested_conctete)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected {} type, got {}", demangle(typeid(Type).name()), concrete_type->getName());

        nested_types.push_back(type_with_nested_conctete->getNestedType());
    }

    return std::make_shared<Type>(
        getLeastCommonTypeForDynamicColumnsImpl(
            type.getNestedType(), nested_types, check_ambiguos_paths));
}

static DataTypePtr getLeastCommonTypeForTuple(
    const DataTypeTuple & type, const DataTypes & concrete_types, bool check_ambiguos_paths)
{
    const auto & element_types = type.getElements();
    DataTypes new_element_types(element_types.size());

    for (size_t i = 0; i < element_types.size(); ++i)
    {
        DataTypes concrete_element_types;
        concrete_element_types.reserve(concrete_types.size());

        for (const auto & type_concrete : concrete_types)
        {
            const auto * type_tuple_conctete = typeid_cast<const DataTypeTuple *>(type_concrete.get());
            if (!type_tuple_conctete)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected Tuple type, got {}", type_concrete->getName());

            concrete_element_types.push_back(type_tuple_conctete->getElement(i));
        }

        new_element_types[i] = getLeastCommonTypeForDynamicColumnsImpl(
            element_types[i], concrete_element_types, check_ambiguos_paths);
    }

    return recreateTupleWithElements(type, new_element_types);
}

static DataTypePtr getLeastCommonTypeForDynamicColumnsImpl(
    const DataTypePtr & type_in_storage, const DataTypes & concrete_types, bool check_ambiguos_paths)
{
    if (!type_in_storage->hasDynamicSubcolumns())
        return type_in_storage;

    if (isObject(type_in_storage))
        return getLeastCommonTypeForObject(concrete_types, check_ambiguos_paths);

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(type_in_storage.get()))
        return getLeastCommonTypeForColumnWithNestedType(*type_array, concrete_types, check_ambiguos_paths);

    if (const auto * type_map = typeid_cast<const DataTypeMap *>(type_in_storage.get()))
        return getLeastCommonTypeForColumnWithNestedType(*type_map, concrete_types, check_ambiguos_paths);

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_in_storage.get()))
        return getLeastCommonTypeForTuple(*type_tuple, concrete_types, check_ambiguos_paths);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Type {} unexpectedly has dynamic columns", type_in_storage->getName());
}

DataTypePtr getLeastCommonTypeForDynamicColumns(
    const DataTypePtr & type_in_storage, const DataTypes & concrete_types, bool check_ambiguos_paths)
{
    if (concrete_types.empty())
        return nullptr;

    bool all_equal = true;
    for (size_t i = 1; i < concrete_types.size(); ++i)
    {
        if (!concrete_types[i]->equals(*concrete_types[0]))
        {
            all_equal = false;
            break;
        }
    }

    if (all_equal)
        return concrete_types[0];

    return getLeastCommonTypeForDynamicColumnsImpl(type_in_storage, concrete_types, check_ambiguos_paths);
}

DataTypePtr createConcreteEmptyDynamicColumn(const DataTypePtr & type_in_storage)
{
    if (!type_in_storage->hasDynamicSubcolumns())
        return type_in_storage;

    if (isObject(type_in_storage))
        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeUInt8>()}, Names{ColumnObject::COLUMN_NAME_DUMMY});

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(type_in_storage.get()))
        return std::make_shared<DataTypeArray>(
            createConcreteEmptyDynamicColumn(type_array->getNestedType()));

    if (const auto * type_map = typeid_cast<const DataTypeMap *>(type_in_storage.get()))
        return std::make_shared<DataTypeMap>(
            createConcreteEmptyDynamicColumn(type_map->getNestedType()));

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_in_storage.get()))
    {
        const auto & elements = type_tuple->getElements();
        DataTypes new_elements;
        new_elements.reserve(elements.size());

        for (const auto & element : elements)
            new_elements.push_back(createConcreteEmptyDynamicColumn(element));

        return recreateTupleWithElements(*type_tuple, new_elements);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Type {} unexpectedly has dynamic columns", type_in_storage->getName());
}

bool hasDynamicSubcolumns(const ColumnsDescription & columns)
{
    return std::any_of(columns.begin(), columns.end(),
        [](const auto & column)
        {
            return column.type->hasDynamicSubcolumns();
        });
}

void extendObjectColumns(NamesAndTypesList & columns_list, const ColumnsDescription & object_columns, bool with_subcolumns)
{
    NamesAndTypesList subcolumns_list;
    for (auto & column : columns_list)
    {
        auto object_column = object_columns.tryGetColumn(GetColumnsOptions::All, column.name);
        if (object_column)
        {
            column.type = object_column->type;

            if (with_subcolumns)
                subcolumns_list.splice(subcolumns_list.end(), object_columns.getSubcolumns(column.name));
        }
    }

    columns_list.splice(columns_list.end(), std::move(subcolumns_list));
}

void updateObjectColumns(
    ColumnsDescription & object_columns,
    const ColumnsDescription & storage_columns,
    const NamesAndTypesList & new_columns)
{
    for (const auto & new_column : new_columns)
    {
        auto object_column = object_columns.tryGetColumn(GetColumnsOptions::All, new_column.name);
        if (object_column && !object_column->type->equals(*new_column.type))
        {
            auto storage_column = storage_columns.getColumn(GetColumnsOptions::All, new_column.name);
            object_columns.modify(new_column.name, [&](auto & column)
            {
                column.type = getLeastCommonTypeForDynamicColumns(storage_column.type, {object_column->type, new_column.type});
            });
        }
    }
}

namespace
{

void flattenTupleImpl(
    PathInDataBuilder & builder,
    DataTypePtr type,
    std::vector<PathInData::Parts> & new_paths,
    DataTypes & new_types)
{
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_names = type_tuple->getElementNames();
        const auto & tuple_types = type_tuple->getElements();

        for (size_t i = 0; i < tuple_names.size(); ++i)
        {
            builder.append(tuple_names[i], false);
            flattenTupleImpl(builder, tuple_types[i], new_paths, new_types);
            builder.popBack();
        }
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        PathInDataBuilder element_builder;
        std::vector<PathInData::Parts> element_paths;
        DataTypes element_types;

        flattenTupleImpl(element_builder, type_array->getNestedType(), element_paths, element_types);
        assert(element_paths.size() == element_types.size());

        for (size_t i = 0; i < element_paths.size(); ++i)
        {
            builder.append(element_paths[i], true);
            new_paths.emplace_back(builder.getParts());
            new_types.emplace_back(std::make_shared<DataTypeArray>(element_types[i]));
            builder.popBack(element_paths[i].size());
        }
    }
    else
    {
        new_paths.emplace_back(builder.getParts());
        new_types.emplace_back(type);
    }
}

/// @offsets_columns are used as stack of array offsets and allows to recreate Array columns.
void flattenTupleImpl(const ColumnPtr & column, Columns & new_columns, Columns & offsets_columns)
{
    if (const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        const auto & subcolumns = column_tuple->getColumns();
        for (const auto & subcolumn : subcolumns)
            flattenTupleImpl(subcolumn, new_columns, offsets_columns);
    }
    else if (const auto * column_array = checkAndGetColumn<ColumnArray>(column.get()))
    {
        offsets_columns.push_back(column_array->getOffsetsPtr());
        flattenTupleImpl(column_array->getDataPtr(), new_columns, offsets_columns);
        offsets_columns.pop_back();
    }
    else
    {
        if (!offsets_columns.empty())
        {
            auto new_column = ColumnArray::create(column, offsets_columns.back());
            for (auto it = offsets_columns.rbegin() + 1; it != offsets_columns.rend(); ++it)
                new_column = ColumnArray::create(new_column, *it);

            new_columns.push_back(std::move(new_column));
        }
        else
        {
            new_columns.push_back(column);
        }
    }
}

DataTypePtr reduceNumberOfDimensions(DataTypePtr type, size_t dimensions_to_reduce)
{
    while (dimensions_to_reduce--)
    {
        const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
        if (!type_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not enough dimensions to reduce");

        type = type_array->getNestedType();
    }

    return type;
}

ColumnPtr reduceNumberOfDimensions(ColumnPtr column, size_t dimensions_to_reduce)
{
    while (dimensions_to_reduce--)
    {
        const auto * column_array = typeid_cast<const ColumnArray *>(column.get());
        if (!column_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not enough dimensions to reduce");

        column = column_array->getDataPtr();
    }

    return column;
}

/// We save intermediate column, type and number of array
/// dimensions for each intermediate node in path in subcolumns tree.
struct ColumnWithTypeAndDimensions
{
    ColumnPtr column;
    DataTypePtr type;
    size_t array_dimensions;
};

using SubcolumnsTreeWithColumns = SubcolumnsTree<ColumnWithTypeAndDimensions>;
using Node = SubcolumnsTreeWithColumns::Node;

/// Creates data type and column from tree of subcolumns.
ColumnWithTypeAndDimensions createTypeFromNode(const Node & node)
{
    auto collect_tuple_elemets = [](const auto & children)
    {
        if (children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create type from empty Tuple or Nested node");

        std::vector<std::tuple<String, ColumnWithTypeAndDimensions>> tuple_elements;
        tuple_elements.reserve(children.size());
        for (const auto & [name, child] : children)
        {
            assert(child);
            auto column = createTypeFromNode(*child);
            tuple_elements.emplace_back(name, std::move(column));
        }

        /// Sort to always create the same type for the same set of subcolumns.
        ::sort(tuple_elements.begin(), tuple_elements.end(),
            [](const auto & lhs, const auto & rhs) { return std::get<0>(lhs) < std::get<0>(rhs); });

        auto tuple_names = extractVector<0>(tuple_elements);
        auto tuple_columns = extractVector<1>(tuple_elements);

        return std::make_tuple(std::move(tuple_names), std::move(tuple_columns));
    };

    if (node.kind == Node::SCALAR)
    {
        return node.data;
    }
    else if (node.kind == Node::NESTED)
    {
        auto [tuple_names, tuple_columns] = collect_tuple_elemets(node.children);

        Columns offsets_columns;
        offsets_columns.reserve(tuple_columns[0].array_dimensions + 1);

        /// If we have a Nested node and child node with anonymous array levels
        /// we need to push a Nested type through all array levels.
        /// Example: { "k1": [[{"k2": 1, "k3": 2}] } should be parsed as
        /// `k1 Array(Nested(k2 Int, k3 Int))` and k1 is marked as Nested
        /// and `k2` and `k3` has anonymous_array_level = 1 in that case.

        const auto & current_array = assert_cast<const ColumnArray &>(*node.data.column);
        offsets_columns.push_back(current_array.getOffsetsPtr());

        auto first_column = tuple_columns[0].column;
        for (size_t i = 0; i < tuple_columns[0].array_dimensions; ++i)
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*first_column);
            offsets_columns.push_back(column_array.getOffsetsPtr());
            first_column = column_array.getDataPtr();
        }

        size_t num_elements = tuple_columns.size();
        Columns tuple_elements_columns(num_elements);
        DataTypes tuple_elements_types(num_elements);

        /// Reduce extra array dimensions to get columns and types of Nested elements.
        for (size_t i = 0; i < num_elements; ++i)
        {
            assert(tuple_columns[i].array_dimensions == tuple_columns[0].array_dimensions);
            tuple_elements_columns[i] = reduceNumberOfDimensions(tuple_columns[i].column, tuple_columns[i].array_dimensions);
            tuple_elements_types[i] = reduceNumberOfDimensions(tuple_columns[i].type, tuple_columns[i].array_dimensions);
        }

        auto result_column = ColumnArray::create(ColumnTuple::create(tuple_elements_columns), offsets_columns.back());
        auto result_type = createNested(tuple_elements_types, tuple_names);

        /// Recreate result Array type and Array column.
        for (auto it = offsets_columns.rbegin() + 1; it != offsets_columns.rend(); ++it)
        {
            result_column = ColumnArray::create(result_column, *it);
            result_type = std::make_shared<DataTypeArray>(result_type);
        }

        return {result_column, result_type, tuple_columns[0].array_dimensions};
    }
    else
    {
        auto [tuple_names, tuple_columns] = collect_tuple_elemets(node.children);

        size_t num_elements = tuple_columns.size();
        Columns tuple_elements_columns(num_elements);
        DataTypes tuple_elements_types(num_elements);

        for (size_t i = 0; i < tuple_columns.size(); ++i)
        {
            assert(tuple_columns[i].array_dimensions == tuple_columns[0].array_dimensions);
            tuple_elements_columns[i] = tuple_columns[i].column;
            tuple_elements_types[i] = tuple_columns[i].type;
        }

        auto result_column = ColumnTuple::create(tuple_elements_columns);
        auto result_type = std::make_shared<DataTypeTuple>(tuple_elements_types, tuple_names);

        return {result_column, result_type, tuple_columns[0].array_dimensions};
    }
}

}

std::pair<PathsInData, DataTypes> flattenTuple(const DataTypePtr & type)
{
    std::vector<PathInData::Parts> new_path_parts;
    DataTypes new_types;
    PathInDataBuilder builder;

    flattenTupleImpl(builder, type, new_path_parts, new_types);

    PathsInData new_paths(new_path_parts.begin(), new_path_parts.end());
    return {new_paths, new_types};
}

ColumnPtr flattenTuple(const ColumnPtr & column)
{
    Columns new_columns;
    Columns offsets_columns;

    flattenTupleImpl(column, new_columns, offsets_columns);
    return ColumnTuple::create(new_columns);
}

DataTypePtr unflattenTuple(const PathsInData & paths, const DataTypes & tuple_types)
{
    assert(paths.size() == tuple_types.size());
    Columns tuple_columns;
    tuple_columns.reserve(tuple_types.size());
    for (const auto & type : tuple_types)
        tuple_columns.emplace_back(type->createColumn());

    return unflattenTuple(paths, tuple_types, tuple_columns).second;
}

std::pair<ColumnPtr, DataTypePtr> unflattenObjectToTuple(const ColumnObject & column)
{
    const auto & subcolumns = column.getSubcolumns();

    if (subcolumns.empty())
    {
        auto type = std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeUInt8>()},
            Names{ColumnObject::COLUMN_NAME_DUMMY});

        return {type->createColumn()->cloneResized(column.size()), type};
    }

    PathsInData paths;
    DataTypes types;
    Columns columns;

    paths.reserve(subcolumns.size());
    types.reserve(subcolumns.size());
    columns.reserve(subcolumns.size());

    for (const auto & entry : subcolumns)
    {
        paths.emplace_back(entry->path);
        types.emplace_back(entry->data.getLeastCommonType());
        columns.emplace_back(entry->data.getFinalizedColumnPtr());
    }

    return unflattenTuple(paths, types, columns);
}

std::pair<ColumnPtr, DataTypePtr> unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns)
{
    assert(paths.size() == tuple_types.size());
    assert(paths.size() == tuple_columns.size());

    if (paths.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unflatten empty Tuple");

    /// We add all paths to the subcolumn tree and then create a type from it.
    /// The tree stores column, type and number of array dimensions
    /// for each intermediate node.
    SubcolumnsTreeWithColumns tree;

    for (size_t i = 0; i < paths.size(); ++i)
    {
        auto column = tuple_columns[i];
        auto type = tuple_types[i];

        const auto & parts = paths[i].getParts();
        size_t num_parts = parts.size();

        size_t pos = 0;
        tree.add(paths[i], [&](Node::Kind kind, bool exists) -> std::shared_ptr<Node>
            {
                if (pos >= num_parts)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Not enough name parts for path {}. Expected at least {}, got {}",
                            paths[i].getPath(), pos + 1, num_parts);

                size_t array_dimensions = kind == Node::NESTED ? 1 : parts[pos].anonymous_array_level;
                ColumnWithTypeAndDimensions current_column{column, type, array_dimensions};

                /// Get type and column for next node.
                if (array_dimensions)
                {
                    type = reduceNumberOfDimensions(type, array_dimensions);
                    column = reduceNumberOfDimensions(column, array_dimensions);
                }

                ++pos;
                if (exists)
                    return nullptr;

                return kind == Node::SCALAR
                    ? std::make_shared<Node>(kind, current_column, paths[i])
                    : std::make_shared<Node>(kind, current_column);
            });
    }

    auto [column, type, _] = createTypeFromNode(tree.getRoot());
    return std::make_pair(std::move(column), std::move(type));
}

static void addConstantToWithClause(const ASTPtr & query, const String & column_name, const DataTypePtr & data_type)
{
    auto & select = query->as<ASTSelectQuery &>();
    if (!select.with())
        select.setExpression(ASTSelectQuery::Expression::WITH, std::make_shared<ASTExpressionList>());

    /// TODO: avoid materialize
    auto node = makeASTFunction("materialize",
        makeASTFunction("CAST",
            std::make_shared<ASTLiteral>(data_type->getDefault()),
            std::make_shared<ASTLiteral>(data_type->getName())));

    node->alias = column_name;
    node->prefer_alias_to_column_name = true;
    select.with()->children.push_back(std::move(node));
}

/// @expected_columns and @available_columns contain descriptions
/// of extended Object columns.
void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query)
{
    NamesAndTypes missed_names_types;

    /// Find all subcolumns that are in @expected_columns, but not in @available_columns.
    for (const auto & column : available_columns)
    {
        auto expected_column = expected_columns.getColumn(GetColumnsOptions::All, column.name);

        /// Extract all paths from both descriptions to easily check existence of subcolumns.
        auto [available_paths, available_types] = flattenTuple(column.type);
        auto [expected_paths, expected_types] = flattenTuple(expected_column.type);

        auto extract_names_and_types = [&column](const auto & paths, const auto & types)
        {
            NamesAndTypes res;
            res.reserve(paths.size());
            for (size_t i = 0; i < paths.size(); ++i)
            {
                auto full_name = Nested::concatenateName(column.name, paths[i].getPath());
                res.emplace_back(full_name, types[i]);
            }

            ::sort(res.begin(), res.end());
            return res;
        };

        auto available_names_types = extract_names_and_types(available_paths, available_types);
        auto expected_names_types = extract_names_and_types(expected_paths, expected_types);

        std::set_difference(
            expected_names_types.begin(), expected_names_types.end(),
            available_names_types.begin(), available_names_types.end(),
            std::back_inserter(missed_names_types),
            [](const auto & lhs, const auto & rhs) { return lhs.name < rhs.name; });
    }

    if (missed_names_types.empty())
        return;

    IdentifierNameSet identifiers;
    query->collectIdentifierNames(identifiers);

    /// Replace missed subcolumns to default literals of theirs type.
    for (const auto & [name, type] : missed_names_types)
        if (identifiers.contains(name))
            addConstantToWithClause(query, name, type);
}

Field FieldVisitorReplaceScalars::operator()(const Array & x) const
{
    if (num_dimensions_to_keep == 0)
        return replacement;

    const size_t size = x.size();
    Array res(size);
    for (size_t i = 0; i < size; ++i)
        res[i] = applyVisitor(FieldVisitorReplaceScalars(replacement, num_dimensions_to_keep - 1), x[i]);
    return res;
}

size_t FieldVisitorToNumberOfDimensions::operator()(const Array & x)
{
    const size_t size = x.size();
    size_t dimensions = 0;

    for (size_t i = 0; i < size; ++i)
    {
        size_t element_dimensions = applyVisitor(*this, x[i]);
        if (i > 0 && element_dimensions != dimensions)
            need_fold_dimension = true;

        dimensions = std::max(dimensions, element_dimensions);
    }

    return 1 + dimensions;
}

Field FieldVisitorFoldDimension::operator()(const Array & x) const
{
    if (num_dimensions_to_fold == 0)
        return x;

    const size_t size = x.size();
    Array res(size);
    for (size_t i = 0; i < size; ++i)
        res[i] = applyVisitor(FieldVisitorFoldDimension(num_dimensions_to_fold - 1), x[i]);

    return res;
}

void setAllObjectsToDummyTupleType(NamesAndTypesList & columns)
{
    for (auto & column : columns)
        if (column.type->hasDynamicSubcolumns())
            column.type = createConcreteEmptyDynamicColumn(column.type);
}

}
