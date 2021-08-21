#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
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

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
}

static const IDataType * getTypeObject(const DataTypePtr & type)
{
    return typeid_cast<const DataTypeObject *>(type.get());
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

DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension)
{
    for (size_t i = 0; i < dimension; ++i)
        type = std::make_shared<DataTypeArray>(std::move(type));
    return type;
}

DataTypePtr getDataTypeByColumn(const IColumn & column)
{
    auto idx = column.getDataType();
    if (WhichDataType(idx).isSimple())
        return DataTypeFactory::instance().get(getTypeName(idx));

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
        if (const auto * type_object = getTypeObject(name_type.type))
        {
            auto & column = block.getByName(name_type.name);

            if (!getTypeObject(column.type))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Type for column '{}' mismatch in columns list and in block. In list: {}, in block: {}",
                    name_type.name, name_type.type->getName(), column.type->getName());

            const auto & column_object = assert_cast<const ColumnObject &>(*column.column);
            const auto & subcolumns_map = column_object.getSubcolumns();

            if (!column_object.isFinalized())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot convert to tuple column '{}' from type {}. Column should be finalized first",
                    name_type.name, name_type.type->getName());

            std::vector<std::tuple<String, DataTypePtr, ColumnPtr>> tuple_elements;
            for (const auto & [key, subcolumn] : subcolumns_map)
                tuple_elements.emplace_back(key,
                    subcolumn.getLeastCommonType(),
                    subcolumn.getFinalizedColumnPtr());

            std::sort(tuple_elements.begin(), tuple_elements.end(),
                [](const auto & lhs, const auto & rhs) { return std::get<0>(lhs) < std::get<0>(rhs); } );

            auto tuple_names = extractVector<0>(tuple_elements);
            auto tuple_types = extractVector<1>(tuple_elements);
            auto tuple_columns = extractVector<2>(tuple_elements);

            auto type_tuple = std::make_shared<DataTypeTuple>(tuple_types, tuple_names);

            auto it = storage_columns_map.find(name_type.name);
            if (it == storage_columns_map.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in storage", name_type.name);

            getLeastCommonTypeForObject({type_tuple, it->second});

            name_type.type = type_tuple;
            column.type = type_tuple;
            column.column = ColumnTuple::create(tuple_columns);
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

void checkObjectHasNoAmbiguosPaths(const Strings & key_names)
{
    for (const auto & name : key_names)
    {
        Strings name_parts;
        boost::split(name_parts, name, boost::is_any_of("."));

        for (const auto & other_name : key_names)
        {
            if (other_name.size() > name.size())
            {
                Strings other_name_parts;
                boost::split(other_name_parts, other_name, boost::is_any_of("."));

                if (isPrefix(name_parts, other_name_parts))
                    throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Data in Object has ambiguous paths: '{}' and '{}", name, other_name);
            }
        }
    }
}

DataTypePtr getLeastCommonTypeForObject(const DataTypes & types)
{
    std::unordered_map<String, DataTypes> subcolumns_types;
    for (const auto & type : types)
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
        if (!type_tuple)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Least common type for object can be deduced only from tuples, but {} given", type->getName());

        const auto & tuple_names = type_tuple->getElementNames();
        const auto & tuple_types = type_tuple->getElements();
        assert(tuple_names.size() == tuple_types.size());

        for (size_t i = 0; i < tuple_names.size(); ++i)
            subcolumns_types[tuple_names[i]].push_back(tuple_types[i]);
    }

    std::vector<std::tuple<String, DataTypePtr>> tuple_elements;
    for (const auto & [name, subtypes] : subcolumns_types)
    {
        assert(!subtypes.empty());
        if (name == ColumnObject::COLUMN_NAME_DUMMY)
            continue;

        size_t first_dim = getNumberOfDimensions(*subtypes[0]);
        for (size_t i = 1; i < subtypes.size(); ++i)
            if (first_dim != getNumberOfDimensions(*subtypes[i]))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Uncompatible types of subcolumn '{}': {} and {}",
                    name, subtypes[0]->getName(), subtypes[i]->getName());

        tuple_elements.emplace_back(name, getLeastSupertype(subtypes, /*allow_conversion_to_string=*/ true));
    }

    if (tuple_elements.empty())
        tuple_elements.emplace_back(ColumnObject::COLUMN_NAME_DUMMY, std::make_shared<DataTypeUInt8>());

    std::sort(tuple_elements.begin(), tuple_elements.end(),
        [](const auto & lhs, const auto & rhs) { return std::get<0>(lhs) < std::get<0>(rhs); } );

    auto tuple_names = extractVector<0>(tuple_elements);
    auto tuple_types = extractVector<1>(tuple_elements);

    checkObjectHasNoAmbiguosPaths(tuple_names);

    return std::make_shared<DataTypeTuple>(tuple_types, tuple_names);
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
