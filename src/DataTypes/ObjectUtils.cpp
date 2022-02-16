#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitors.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/Operators.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <base/EnumReflection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
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
    const DataTypeArray * last_array = nullptr;
    const IDataType * current_type = type.get();
    while (const auto * type_array = typeid_cast<const DataTypeArray *>(current_type))
    {
        current_type = type_array->getNestedType().get();
        last_array = type_array;
    }

    return last_array ? last_array->getNestedType() : type;
}

ColumnPtr getBaseColumnOfArray(const ColumnPtr & column)
{
    const ColumnArray * last_array = nullptr;
    const IColumn * current_column = column.get();
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
    if (WhichDataType(idx).isSimple())
        return DataTypeFactory::instance().get(String(magic_enum::enum_name(idx)));

    if (const auto * column_array = checkAndGetColumn<ColumnArray>(&column))
        return std::make_shared<DataTypeArray>(getDataTypeByColumn(column_array->getData()));

    if (const auto * column_nullable = checkAndGetColumn<ColumnNullable>(&column))
        return makeNullable(getDataTypeByColumn(column_nullable->getNestedColumn()));

    /// TODO: add more types.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get data type of column {}", column.getFamilyName());
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

void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block, const NamesAndTypesList & extended_storage_columns)
{
    std::unordered_map<String, DataTypePtr> storage_columns_map;
    for (const auto & [name, type] : extended_storage_columns)
        storage_columns_map[name] = type;

    for (auto & name_type : columns_list)
    {
        if (isObject(name_type.type))
        {
            auto & column = block.getByName(name_type.name);

            if (!isObject(column.type))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Type for column '{}' mismatch in columns list and in block. In list: {}, in block: {}",
                    name_type.name, name_type.type->getName(), column.type->getName());

            const auto & column_object = assert_cast<const ColumnObject &>(*column.column);
            const auto & subcolumns_map = column_object.getSubcolumns();

            if (!column_object.isFinalized())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot convert to tuple column '{}' from type {}. Column should be finalized first",
                    name_type.name, name_type.type->getName());

            PathsInData tuple_paths;
            DataTypes tuple_types;
            Columns tuple_columns;

            for (const auto & entry : subcolumns_map)
            {
                tuple_paths.emplace_back(entry->path);
                tuple_types.emplace_back(entry->data.getLeastCommonType());
                tuple_columns.emplace_back(entry->data.getFinalizedColumnPtr());
            }

            auto it = storage_columns_map.find(name_type.name);
            if (it == storage_columns_map.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in storage", name_type.name);

            std::tie(column.column, column.type) = unflattenTuple(tuple_paths, tuple_types, tuple_columns);
            name_type.type = column.type;

            getLeastCommonTypeForObject({column.type, it->second}, true);
        }
    }
}

static bool isPrefix(const PathInData::Parts & prefix, const PathInData::Parts & strings)
{
    if (prefix.size() > strings.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
        if (prefix[i].key != strings[i].key)
            return false;
    return true;
}

void checkObjectHasNoAmbiguosPaths(const PathsInData & paths)
{
    size_t size = paths.size();
    std::vector<PathInData::Parts> names_parts(size);

    for (size_t i = 0; i < size; ++i)
        names_parts[i] = paths[i].getParts();

    for (size_t i = 0; i < size; ++i)
    {
        for (size_t j = 0; j < i; ++j)
        {
            if (isPrefix(names_parts[i], names_parts[j]) || isPrefix(names_parts[j], names_parts[i]))
                throw Exception(ErrorCodes::DUPLICATE_COLUMN,
                    "Data in Object has ambiguous paths: '{}' and '{}'",
                    paths[i].getPath(), paths[i].getPath());
        }
    }
}

DataTypePtr getLeastCommonTypeForObject(const DataTypes & types, bool check_ambiguos_paths)
{
    if (types.empty())
        return nullptr;

    bool all_equal = true;
    for (size_t i = 1; i < types.size(); ++i)
    {
        if (!types[i]->equals(*types[0]))
        {
            all_equal = false;
            break;
        }
    }

    if (all_equal)
        return types[0];

    std::unordered_map<PathInData, DataTypes, PathInData::Hash> subcolumns_types;

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
        tuple_types.emplace_back(getLeastSupertype(subtypes, /*allow_conversion_to_string=*/ true));
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

NameSet getNamesOfObjectColumns(const NamesAndTypesList & columns_list)
{
    NameSet res;
    for (const auto & [name, type] : columns_list)
        if (isObject(type))
            res.insert(name);

    return res;
}

bool hasObjectColumns(const ColumnsDescription & columns)
{
    return std::any_of(columns.begin(), columns.end(), [](const auto & column) { return isObject(column.type); });
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

void updateObjectColumns(ColumnsDescription & object_columns, const NamesAndTypesList & new_columns)
{
    for (const auto & new_column : new_columns)
    {
        auto object_column = object_columns.tryGetPhysical(new_column.name);
        if (object_column && !object_column->type->equals(*new_column.type))
        {
            object_columns.modify(new_column.name, [&](auto & column)
            {
                column.type = getLeastCommonTypeForObject({object_column->type, new_column.type});
            });
        }
    }
}

namespace
{

void flattenTupleImpl(PathInDataBuilder & builder, DataTypePtr type, size_t array_level, PathsInData & new_paths, DataTypes & new_types)
{
    bool is_nested = isNested(type);

    if (is_nested)
        type = assert_cast<const DataTypeArray &>(*type).getNestedType();

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_names = type_tuple->getElementNames();
        const auto & tuple_types = type_tuple->getElements();

        for (size_t i = 0; i < tuple_names.size(); ++i)
        {
            builder.append(tuple_names[i], is_nested);
            flattenTupleImpl(builder, tuple_types[i], array_level + is_nested, new_paths, new_types);
            builder.popBack();
        }
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        flattenTupleImpl(builder, type_array->getNestedType(), array_level + 1, new_paths, new_types);
    }
    else
    {
        new_paths.emplace_back(builder.getParts());
        new_types.push_back(createArrayOfType(type, array_level));
    }
}

void flattenTupleImpl(const ColumnPtr & column, Columns & new_columns, Columns & offsets_columns)
{
    if (const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
    {
        const auto & subcolumns = column_tuple->getColumns();
        for (const auto & subcolumn : subcolumns)
            flattenTupleImpl(subcolumn, new_columns,offsets_columns);
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
            for (ssize_t i = static_cast<ssize_t>(offsets_columns.size()) - 2; i >= 0; --i)
                new_column = ColumnArray::create(new_column, offsets_columns[i]);

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

struct ColumnWithTypeAndDimensions
{
    ColumnPtr column;
    DataTypePtr type;
    size_t array_dimensions;
};

using SubcolumnsTreeWithTypes = SubcolumnsTree<ColumnWithTypeAndDimensions, ColumnWithTypeAndDimensions>;
using Node = SubcolumnsTreeWithTypes::Node;
using Leaf = SubcolumnsTreeWithTypes::Leaf;

std::pair<ColumnPtr, DataTypePtr> createTypeFromNode(const Node * node)
{
    auto collect_tuple_elemets = [](const auto & children)
    {
        std::vector<std::tuple<String, ColumnPtr, DataTypePtr>> tuple_elements;
        tuple_elements.reserve(children.size());
        for (const auto & [name, child] : children)
        {
            auto [column, type] = createTypeFromNode(child.get());
            tuple_elements.emplace_back(name, std::move(column), std::move(type));
        }

        std::sort(tuple_elements.begin(), tuple_elements.end(),
            [](const auto & lhs, const auto & rhs) { return std::get<0>(lhs) < std::get<0>(rhs); });

        auto tuple_names = extractVector<0>(tuple_elements);
        auto tuple_columns = extractVector<1>(tuple_elements);
        auto tuple_types = extractVector<2>(tuple_elements);

        return std::make_tuple(tuple_names, tuple_columns, tuple_types);
    };

    if (node->kind == Node::SCALAR)
    {
        const auto * leaf = typeid_cast<const Leaf *>(node);
        return {leaf->data.column, leaf->data.type};
    }
    else if (node->kind == Node::NESTED)
    {
        Columns offsets_columns;
        ColumnPtr current_column = node->data.column;

        assert(node->data.array_dimensions > 0);
        offsets_columns.reserve(node->data.array_dimensions);

        for (size_t i = 0; i < node->data.array_dimensions; ++i)
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*current_column);

            offsets_columns.push_back(column_array.getOffsetsPtr());
            current_column = column_array.getDataPtr();
        }

        auto [tuple_names, tuple_columns, tuple_types] = collect_tuple_elemets(node->children);

        auto result_column = ColumnArray::create(ColumnTuple::create(tuple_columns), offsets_columns.back());
        auto result_type = createNested(tuple_types, tuple_names);

        for (ssize_t i = static_cast<ssize_t>(offsets_columns.size()) - 2; i >= 0; --i)
        {
            result_column = ColumnArray::create(result_column, offsets_columns[i]);
            result_type = std::make_shared<DataTypeArray>(result_type);
        }

        return {result_column, result_type};
    }
    else
    {
        auto [tuple_names, tuple_columns, tuple_types] = collect_tuple_elemets(node->children);

        auto result_column = ColumnTuple::create(tuple_columns);
        auto result_type = std::make_shared<DataTypeTuple>(tuple_types, tuple_names);

        return {result_column, result_type};
    }
}

}

std::pair<PathsInData, DataTypes> flattenTuple(const DataTypePtr & type)
{
    PathsInData new_paths;
    DataTypes new_types;
    PathInDataBuilder builder;

    flattenTupleImpl(builder, type, 0, new_paths, new_types);
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

std::pair<ColumnPtr, DataTypePtr> unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns)
{
    assert(paths.size() == tuple_types.size());
    assert(paths.size() == tuple_columns.size());

    SubcolumnsTreeWithTypes tree;

    for (size_t i = 0; i < paths.size(); ++i)
    {
        auto column = tuple_columns[i];
        auto type = tuple_types[i];

        const auto & parts = paths[i].getParts();

        size_t num_parts = parts.size();
        size_t nested_level = std::count_if(parts.begin(), parts.end(), [](const auto & part) { return part.is_nested; });
        size_t array_level = getNumberOfDimensions(*type);

        if (array_level < nested_level)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Number of dimensions ({}) is less than number Nested types ({}) for path {}",
                array_level, nested_level, paths[i].getPath());

        size_t pos = 0;
        tree.add(paths[i], [&](Node::Kind kind, bool exists) -> std::shared_ptr<Node>
            {
                if (pos >= num_parts)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Not enough name parts for path {}. Expected at least {}, got {}",
                            paths[i].getPath(), pos + 1, num_parts);

                ColumnWithTypeAndDimensions current_column;
                if (kind == Node::NESTED)
                {
                    size_t dimensions_to_reduce = array_level - nested_level;
                    assert(parts[pos].is_nested);

                    ++dimensions_to_reduce;
                    --nested_level;

                    current_column = ColumnWithTypeAndDimensions{column, type, dimensions_to_reduce};

                    if (dimensions_to_reduce)
                    {
                        type = reduceNumberOfDimensions(type, dimensions_to_reduce);
                        column = reduceNumberOfDimensions(column, dimensions_to_reduce);
                    }

                    array_level -= dimensions_to_reduce;
                }
                else
                    current_column = ColumnWithTypeAndDimensions{column, type, 0};

                ++pos;

                if (exists)
                    return nullptr;

                return kind == Node::SCALAR
                    ? std::make_shared<Leaf>(paths[i], current_column)
                    : std::make_shared<Node>(kind, current_column);
            });
    }

    return createTypeFromNode(tree.getRoot());
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

void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query)
{
    NamesAndTypes missed_names_types;
    for (const auto & column : available_columns)
    {
        auto expected_column = expected_columns.getColumn(GetColumnsOptions::All, column.name);

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

            std::sort(res.begin(), res.end());
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
    for (const auto & [name, type] : missed_names_types)
        if (identifiers.count(name))
            addConstantToWithClause(query, name, type);
}

void finalizeObjectColumns(MutableColumns & columns)
{
    for (auto & column : columns)
        if (auto * column_object = typeid_cast<ColumnObject *>(column.get()))
            column_object->finalize();
}

}
