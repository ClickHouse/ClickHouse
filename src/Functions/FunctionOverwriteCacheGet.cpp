#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/QualifiedTableName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/castColumn.h>

#include <Storages/StorageOverwriteCache.h>
#include <Storages/TableLockHolder.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TYPE_MISMATCH;
}

namespace
{

using StorageOverwriteCachePtr = std::shared_ptr<StorageOverwriteCache>;

class ExecutableOverwriteCacheGet final : public IExecutableFunction
{
public:
    ExecutableOverwriteCacheGet(
        String function_name_,
        TableLockHolder table_lock_,
        StorageOverwriteCachePtr storage_,
        String attribute_,
        bool or_null_,
        DataTypes key_types_)
        : function_name(std::move(function_name_))
        , table_lock(std::move(table_lock_))
        , storage(std::move(storage_))
        , attribute(std::move(attribute_))
        , or_null(or_null_)
        , key_types(std::move(key_types_))
    {
    }

    String getName() const override { return function_name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnsWithTypeAndName keys;
        keys.reserve(arguments.size() - 2);
        for (size_t index = 2; index < arguments.size(); ++index)
        {
            auto key = arguments[index];
            const auto & expected_type = key_types[index - 2];
            if (!key.type->equals(*expected_type))
            {
                key.column = castColumn(key, expected_type);
                key.type = expected_type;
            }
            keys.push_back(std::move(key));
        }

        PaddedPODArray<UInt8> found_map;
        IColumn::Offsets offsets;
        auto result = storage->getByKeys(keys, {attribute}, found_map, offsets);
        auto columns = result.detachColumns();
        auto result_column = IColumn::mutate(std::move(columns.front()));

        if (!or_null)
            return result_column;

        result_column = IColumn::mutate(result_column->convertToFullColumnIfLowCardinality());
        auto missing_map = ColumnUInt8::create();
        auto & missing_data = missing_map->getData();
        missing_data.resize(found_map.size());
        for (size_t row = 0; row < found_map.size(); ++row)
            missing_data[row] = !found_map[row];

        if (auto * nullable = typeid_cast<ColumnNullable *>(result_column.get()))
        {
            nullable->applyNullMap(*missing_map);
            return result_column;
        }
        return ColumnNullable::create(std::move(result_column), std::move(missing_map));
    }

private:
    String function_name;
    TableLockHolder table_lock;
    StorageOverwriteCachePtr storage;
    String attribute;
    bool or_null;
    DataTypes key_types;
};

class OverwriteCacheGetFunction final : public IFunctionBase
{
public:
    OverwriteCacheGetFunction(
        String function_name_,
        ContextPtr context_,
        TableLockHolder table_lock_,
        StorageOverwriteCachePtr storage_,
        String attribute_,
        bool or_null_,
        DataTypes key_types_,
        DataTypes argument_types_,
        DataTypePtr return_type_)
        : function_name(std::move(function_name_))
        , context(std::move(context_))
        , table_lock(std::move(table_lock_))
        , storage(std::move(storage_))
        , attribute(std::move(attribute_))
        , or_null(or_null_)
        , key_types(std::move(key_types_))
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
    {
    }

    String getName() const override { return function_name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        Names columns = storage->getPrimaryKey();
        columns.push_back(attribute);
        context->checkAccess(AccessType::SELECT, storage->getStorageID(), columns);
        return std::make_unique<ExecutableOverwriteCacheGet>(function_name, table_lock, storage, attribute, or_null, key_types);
    }

private:
    String function_name;
    ContextPtr context;
    TableLockHolder table_lock;
    StorageOverwriteCachePtr storage;
    String attribute;
    bool or_null;
    DataTypes key_types;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class OverwriteCacheGetResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    OverwriteCacheGetResolver(String function_name_, bool or_null_, ContextPtr context_)
        : WithContext(std::move(context_))
        , function_name(std::move(function_name_))
        , or_null(or_null_)
    {
    }

    static FunctionOverloadResolverPtr create(String function_name, bool or_null, ContextPtr context_)
    {
        return std::make_unique<OverwriteCacheGetResolver>(std::move(function_name), or_null, std::move(context_));
    }

    String getName() const override { return function_name; }
    bool isDeterministic() const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return {}; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (arguments.size() < 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires a table, an attribute, and at least one key argument",
                function_name);

        const auto * table_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!table_column)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be a constant String table name", function_name);
        const String table_name = table_column->getValue<String>();
        const auto qualified_name = QualifiedTableName::parseFromString(table_name);
        const auto storage_id = getContext()->resolveStorageID({qualified_name.database, qualified_name.table});
        auto table = DatabaseCatalog::instance().getTable(storage_id, std::const_pointer_cast<Context>(getContext()));
        auto storage = std::dynamic_pointer_cast<StorageOverwriteCache>(table);
        if (!storage)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Table {} must use engine `OverwriteCache`", table_name);

        const auto * attribute_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!attribute_column)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be a constant String attribute name",
                function_name);
        const String attribute = attribute_column->getValue<String>();

        const auto & expected_key_types = storage->getKeyColumnTypes();
        if (arguments.size() - 2 != expected_key_types.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires {} key arguments, got {}",
                function_name,
                expected_key_types.size(),
                arguments.size() - 2);
        auto attribute_header = storage->getSampleBlock({attribute});
        auto return_type = attribute_header.getByName(attribute).type;
        if (or_null)
            return_type = makeNullable(removeLowCardinality(return_type));

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        for (size_t index = 0; index < expected_key_types.size(); ++index)
        {
            const auto actual_type = removeLowCardinality(arguments[index + 2].type);
            const auto expected_type = removeLowCardinality(expected_key_types[index]);
            if (!actual_type->equals(*expected_type))
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Key argument {} has type {}, expected {}",
                    index + 1,
                    arguments[index + 2].type->getName(),
                    expected_key_types[index]->getName());
        }

        auto table_lock
            = storage->lockForShare(getContext()->getInitialQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
        return std::make_unique<OverwriteCacheGetFunction>(
            function_name,
            getContext(),
            std::move(table_lock),
            std::move(storage),
            attribute,
            or_null,
            expected_key_types,
            std::move(argument_types),
            std::move(return_type));
    }

private:
    String function_name;
    bool or_null;
};

}

REGISTER_FUNCTION(OverwriteCacheGet)
{
    FunctionDocumentation::Description description = R"(
Looks up a value in an `OverwriteCache` table by its complete composite key.
)";
    FunctionDocumentation::Arguments arguments = {
        {"table", "Qualified `OverwriteCache` table name.", {"const String"}},
        {"value_column", "Column whose value is returned.", {"const String"}},
        {"keys", "Composite key values in the order declared by `KEYS`.", {"Any"}},
    };
    FunctionDocumentation::Examples examples;
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_get
        = {description,
           "overwriteCacheGet(table, value_column, keys)",
           arguments,
           {},
           {"Returns the stored value, or the value column's default when the key is absent.", {"Any"}},
           examples,
           introduced_in,
           category};
    FunctionDocumentation documentation_get_or_null
        = {description,
           "overwriteCacheGetOrNull(table, value_column, keys)",
           arguments,
           {},
           {"Returns the stored value, or `NULL` when the key is absent.", {"Any"}},
           examples,
           introduced_in,
           category};

    factory.registerFunction(
        "overwriteCacheGet",
        [](ContextPtr context) { return OverwriteCacheGetResolver::create("overwriteCacheGet", false, std::move(context)); },
        documentation_get);
    factory.registerFunction(
        "overwriteCacheGetOrNull",
        [](ContextPtr context) { return OverwriteCacheGetResolver::create("overwriteCacheGetOrNull", true, std::move(context)); },
        documentation_get_or_null);
}

}
