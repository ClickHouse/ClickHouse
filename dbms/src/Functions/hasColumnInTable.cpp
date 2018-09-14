#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Storages/getStructureOfRemoteTable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** Usage:
 *  hasColumnInTable(['hostname'[, 'username'[, 'password']],] 'database', 'table', 'column')
 */
class FunctionHasColumnInTable : public IFunction
{
public:
    static constexpr auto name = "hasColumnInTable";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionHasColumnInTable>(context.getGlobalContext());
    }

    explicit FunctionHasColumnInTable(const Context & global_context_) : global_context(global_context_)
    {
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    const Context & global_context;
};


DataTypePtr FunctionHasColumnInTable::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 3 || arguments.size() > 6)
        throw Exception{"Invalid number of arguments for function " + getName(),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    static const std::string arg_pos_description[] = {"First", "Second", "Third", "Fourth", "Fifth", "Sixth"};
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & argument = arguments[i];

        if (!checkColumnConst<ColumnString>(argument.column.get()))
        {
            throw Exception(arg_pos_description[i] + " argument for function " + getName() + " must be const String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    return std::make_shared<DataTypeUInt8>();
}


void FunctionHasColumnInTable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    auto get_string_from_block = [&](size_t column_pos) -> String
    {
        ColumnPtr column = block.getByPosition(column_pos).column;
        const ColumnConst * const_column = checkAndGetColumnConst<ColumnString>(column.get());
        return const_column->getValue<String>();
    };

    size_t arg = 0;
    String host_name;
    String user_name;
    String password;

    if (arguments.size() > 3)
        host_name = get_string_from_block(arguments[arg++]);

    if (arguments.size() > 4)
        user_name = get_string_from_block(arguments[arg++]);

    if (arguments.size() > 5)
        password = get_string_from_block(arguments[arg++]);

    String database_name = get_string_from_block(arguments[arg++]);
    String table_name = get_string_from_block(arguments[arg++]);
    String column_name = get_string_from_block(arguments[arg++]);

    bool has_column;
    if (host_name.empty())
    {
        const StoragePtr & table = global_context.getTable(database_name, table_name);
        has_column = table->hasColumn(column_name);
    }
    else
    {
        std::vector<std::vector<String>> host_names = {{ host_name }};

        auto cluster = std::make_shared<Cluster>(
            global_context.getSettings(),
            host_names,
            !user_name.empty() ? user_name : "default",
            password,
            global_context.getTCPPort(),
            false);

        auto remote_columns = getStructureOfRemoteTable(*cluster, database_name, table_name, global_context);
        has_column = remote_columns.hasPhysical(column_name);
    }

    block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(has_column));
}


void registerFunctionHasColumnInTable(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHasColumnInTable>();
}

}
