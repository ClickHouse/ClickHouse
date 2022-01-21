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
#include <Columns/ColumnTuple.h>
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

ColumnPtr createArrayOfColumn(ColumnPtr column, size_t num_dimensions)
{
    for (size_t i = 0; i < num_dimensions; ++i)
        column = ColumnArray::create(column);
    return column;
}

DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension)
{
    for (size_t i = 0; i < dimension; ++i)
        type = std::make_shared<DataTypeArray>(std::move(type));
    return type;
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

            std::vector<std::tuple<Path, DataTypePtr, ColumnPtr>> tuple_elements;
            tuple_elements.reserve(subcolumns_map.size());
            for (const auto & entry : subcolumns_map)
                tuple_elements.emplace_back(entry->path,
                    entry->column.getLeastCommonType(),
                    entry->column.getFinalizedColumnPtr());

            std::sort(tuple_elements.begin(), tuple_elements.end(),
                [](const auto & lhs, const auto & rhs) { return std::get<0>(lhs).getPath() < std::get<0>(rhs).getPath(); });

            auto tuple_paths = extractVector<0>(tuple_elements);
            auto tuple_types = extractVector<1>(tuple_elements);
            auto tuple_columns = extractVector<2>(tuple_elements);

            auto it = storage_columns_map.find(name_type.name);
            if (it == storage_columns_map.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in storage", name_type.name);

            Strings tuple_names;
            tuple_names.reserve(tuple_paths.size());
            for (const auto & path : tuple_paths)
                tuple_names.push_back(path.getPath());

            auto type_tuple = std::make_shared<DataTypeTuple>(tuple_types, tuple_names);

            getLeastCommonTypeForObject({type_tuple, it->second}, true);

            std::tie(column.type, column.column) = unflattenTuple(tuple_paths, tuple_types, tuple_columns);
            name_type.type = column.type;
        }
    }
}

static bool isPrefix(const Strings & prefix, const Strings & strings)
{
    if (prefix.size() > strings.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
        if (prefix[i] != strings[i])
            return false;
    return true;
}

void checkObjectHasNoAmbiguosPaths(const Paths & paths)
{
    size_t size = paths.size();
    std::vector<Strings> names_parts(size);

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
    std::unordered_map<Path, DataTypes, Path::Hash> subcolumns_types;

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

    std::vector<std::tuple<Path, DataTypePtr>> tuple_elements;
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

        tuple_elements.emplace_back(key, getLeastSupertype(subtypes, /*allow_conversion_to_string=*/ true));
    }

    if (tuple_elements.empty())
        tuple_elements.emplace_back(Path(ColumnObject::COLUMN_NAME_DUMMY), std::make_shared<DataTypeUInt8>());

    std::sort(tuple_elements.begin(), tuple_elements.end(),
        [](const auto & lhs, const auto & rhs) { return std::get<0>(lhs).getPath() < std::get<0>(rhs).getPath(); });

    auto tuple_paths = extractVector<0>(tuple_elements);
    auto tuple_types = extractVector<1>(tuple_elements);

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

namespace
{

struct SubcolumnsHolder
{
    SubcolumnsHolder(
        const std::vector<Path> & paths_,
        const DataTypes & types_,
        const Columns & columns_)
        : paths(paths_)
        , types(types_)
        , columns(columns_)
    {
        parts.reserve(paths.size());
        levels.reserve(paths.size());

        for (const auto & path : paths)
        {
            parts.emplace_back(path.getParts());

            size_t num_parts = path.getNumParts();

            levels.emplace_back();
            levels.back().resize(num_parts);

            for (size_t i = 1; i < num_parts; ++i)
                levels.back()[i] = levels.back()[i - 1] + path.isNested(i - 1);
        }
    }

    std::pair<DataTypePtr, ColumnPtr> reduceNumberOfDimensions(size_t path_idx, size_t key_idx) const
    {
        auto type = types[path_idx];
        auto column = columns[path_idx];

        if (!isArray(type))
            return {type, column};

        size_t type_dimensions = getNumberOfDimensions(*type);
        size_t column_dimensions = getNumberOfDimensions(*column);

        if (levels[path_idx][key_idx] > type_dimensions)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Level of nested ({}) cannot be greater than number of dimensions in array ({})",
                levels[path_idx][key_idx], type_dimensions);

        if (type_dimensions != column_dimensions)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Number of dimensionsin type ({}) is incompatible with number of dimension in column ({})",
                type_dimensions, column_dimensions);

        size_t dimensions_to_reduce = levels[path_idx][key_idx];
        while (dimensions_to_reduce--)
        {
            const auto & type_array = assert_cast<const DataTypeArray &>(*type);
            const auto & column_array = assert_cast<const ColumnArray &>(*column);

            type = type_array.getNestedType();
            column = column_array.getDataPtr();
        }

        return {type, column};
    }

    std::vector<Path> paths;
    DataTypes types;
    Columns columns;

    std::vector<Strings> parts;
    std::vector<std::vector<size_t>> levels;
};


void flattenTupleImpl(Path path, DataTypePtr type, size_t array_level, Paths & new_paths, DataTypes & new_types)
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
            auto next_path = Path::getNext(path, Path(tuple_names[i]), is_nested);
            size_t next_level = array_level + is_nested;
            flattenTupleImpl(next_path, tuple_types[i], next_level, new_paths, new_types);
        }
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        flattenTupleImpl(path, type_array->getNestedType(), array_level + 1, new_paths, new_types);
    }
    else
    {
        new_paths.push_back(path);
        new_types.push_back(createArrayOfType(type, array_level));
    }
}

void unflattenTupleImpl(
    const SubcolumnsHolder & holder,
    size_t from, size_t to, size_t depth,
    Names & new_names, DataTypes & new_types, Columns & new_columns)
{
    size_t start = from;
    for (size_t i = from + 1; i <= to; ++i)
    {
        if (i < to && holder.parts[i].size() <= depth)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unflatten Tuple. Not enough name parts in path");

        if (i == to || holder.parts[i][depth] != holder.parts[start][depth])
        {
            if (i - start > 1)
            {
                Names tuple_names;
                DataTypes tuple_types;
                Columns tuple_columns;

                unflattenTupleImpl(holder, start, i, depth + 1, tuple_names, tuple_types, tuple_columns);

                assert(!tuple_names.empty());
                assert(tuple_names.size() == tuple_types.size());
                assert(tuple_names.size() == tuple_columns.size());

                new_names.push_back(holder.parts[start][depth]);

                if (holder.paths[start].isNested(depth))
                {
                    auto array_column = holder.reduceNumberOfDimensions(start, depth).second;
                    auto offsets_column = assert_cast<const ColumnArray &>(*array_column).getOffsetsPtr();

                    new_types.push_back(createNested(tuple_types, tuple_names));
                    new_columns.push_back(ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), offsets_column));
                }
                else
                {
                    new_types.push_back(std::make_shared<DataTypeTuple>(tuple_types, tuple_names));
                    new_columns.push_back(ColumnTuple::create(std::move(tuple_columns)));
                }
            }
            else
            {
                WriteBufferFromOwnString wb;
                wb << holder.parts[start][depth];
                for (size_t j = depth + 1; j < holder.parts[start].size(); ++j)
                    wb << "." << holder.parts[start][j];

                auto [new_type, new_column] = holder.reduceNumberOfDimensions(start, depth);

                new_names.push_back(wb.str());
                new_types.push_back(std::move(new_type));
                new_columns.push_back(std::move(new_column));
            }

            start = i;
        }
    }
}

}

std::pair<Paths, DataTypes> flattenTuple(const DataTypePtr & type)
{
    Paths new_paths;
    DataTypes new_types;

    flattenTupleImpl({}, type, 0, new_paths, new_types);
    return {new_paths, new_types};
}

DataTypePtr unflattenTuple(const Paths & paths, const DataTypes & tuple_types)
{
    assert(paths.size() == types.size());
    Columns tuple_columns(tuple_types.size());
    for (size_t i = 0; i < tuple_types.size(); ++i)
        tuple_columns[i] = tuple_types[i]->createColumn();

    return unflattenTuple(paths, tuple_types, tuple_columns).first;
}

std::pair<DataTypePtr, ColumnPtr> unflattenTuple(
    const Paths & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns)
{
    assert(paths.size() == types.size());
    assert(paths.size() == columns.size());

    Names new_names;
    DataTypes new_types;
    Columns new_columns;
    SubcolumnsHolder holder(paths, tuple_types, tuple_columns);

    unflattenTupleImpl(holder, 0, paths.size(), 0, new_names, new_types, new_columns);

    return
    {
        std::make_shared<DataTypeTuple>(new_types, new_names),
        ColumnTuple::create(new_columns)
    };
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
    NamesAndTypesList missed;
    for (const auto & column : available_columns)
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(column.type.get());
        assert(type_tuple);

        auto expected_column = expected_columns.getColumn(GetColumnsOptions::All, column.name);
        const auto * expected_type_tuple = typeid_cast<const DataTypeTuple *>(expected_column.type.get());
        assert(expected_type_tuple);

        if (!type_tuple->equals(*expected_type_tuple))
        {
            const auto & names = type_tuple->getElementNames();
            const auto & expected_names = expected_type_tuple->getElementNames();
            const auto & expected_types = expected_type_tuple->getElements();

            NameSet names_set(names.begin(), names.end());

            for (size_t i = 0; i < expected_names.size(); ++i)
            {
                if (!names_set.count(expected_names[i]))
                {
                    auto full_name = Nested::concatenateName(column.name, expected_names[i]);
                    missed.emplace_back(std::move(full_name), expected_types[i]);
                }
            }
        }
    }

    if (missed.empty())
        return;

    IdentifierNameSet identifiers;
    query->collectIdentifierNames(identifiers);
    for (const auto & [name, type] : missed)
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
