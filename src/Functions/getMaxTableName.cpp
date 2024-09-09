#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Core/Field.h>
#include <filesystem>
#include <Common/escapeForFileName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int UNKNOWN_DATABASE;
}

class FunctionGetMaxTableName: public IFunction, WithContext
{
    const String db_name;

public:
    static constexpr auto name = "getMaxTableNameForDatabase";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetMaxTableName>(context_);
    }

    explicit FunctionGetMaxTableName(ContextPtr context_):WithContext(context_)
    {

    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Number of arguments for function {} can't be {}, should be 1", getName(), arguments.size());

        WhichDataType which(arguments[0]);

        if (!which.isStringOrFixedString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected String or FixedString",
                arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt64>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        String suffix = ".sql.detached";
        String create_directory_name = (std::filesystem::path(getContext()->getPath()) / "metadata").string();
        auto max_create_length = pathconf(create_directory_name.data(), _PC_NAME_MAX);
        String dropped_directory_name = (std::filesystem::path(getContext()->getPath()) / "metadata_dropped").string();
        auto max_dropped_length = pathconf(dropped_directory_name.data(), _PC_NAME_MAX);
        size_t allowed_max_length;

        if (max_dropped_length == -1)
            max_dropped_length = NAME_MAX;

        if (max_create_length == -1)
            max_create_length = NAME_MAX;

        //File name to drop is escaped_db_name.escaped_table_name.uuid.sql
        //File name to create is table_name.sql
        auto max_to_create = static_cast<size_t>(max_create_length)  - suffix.length();
        const IColumn * col;

        if (!isColumnConst(*arguments[0].column.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant.", getName());

        String database_name;
        WhichDataType which(arguments[0].type);

        if (which.isString())
            col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        else
            col = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());

        database_name = col->getDataAt(1).toString();

        if (!DatabaseCatalog::instance().isDatabaseExist(database_name))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exist.", database_name);

        //36 is prepared for renaming table operation while dropping
        //max_to_drop = max_dropped_length - length_of(database_name)- length_of(uuid) - lenght_of('sql' + 3 dots)
        auto max_to_drop = static_cast<size_t>(max_dropped_length) - escapeForFileName(database_name).length() - 48;
        allowed_max_length = max_to_create > max_to_drop ? max_to_drop : max_to_create;
        return DataTypeUInt64().createColumnConst(input_rows_count, allowed_max_length);
    }
};


REGISTER_FUNCTION(getMaxTableName)
{
    factory.registerFunction<FunctionGetMaxTableName>();
}

}

