#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Interpreters/Context.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/AccessControlManager.h>
#include <ext/range.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// The currentRowPolicies() function can be called with 0..2 arguments:
/// currentRowPolicies() returns array of tuples (database, table_name, row_policy_name) for all the row policies applied for the current user;
/// currentRowPolicies(table_name) is equivalent to currentRowPolicies(currentDatabase(), table_name);
/// currentRowPolicies(database, table_name) returns array of names of the row policies applied to a specific table and for the current user.
class FunctionCurrentRowPolicies : public IFunction
{
public:
    static constexpr auto name = "currentRowPolicies";

    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionCurrentRowPolicies>(context_); }
    explicit FunctionCurrentRowPolicies(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    void checkNumberOfArgumentsIfVariadic(size_t number_of_arguments) const override
    {
        if (number_of_arguments > 2)
            throw Exception("Number of arguments for function " + String(name) + " doesn't match: passed "
                            + toString(number_of_arguments) + ", should be 0..2",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
        else
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_pos, size_t input_rows_count) override
    {
        if (arguments.empty())
        {
            auto database_column = ColumnString::create();
            auto table_name_column = ColumnString::create();
            auto policy_name_column = ColumnString::create();
            if (auto policies = context.getRowPolicies())
            {
                for (const auto & policy_id : policies->getCurrentPolicyIDs())
                {
                    const auto policy = context.getAccessControlManager().tryRead<RowPolicy>(policy_id);
                    if (policy)
                    {
                        const String database = policy->getDatabase();
                        const String table_name = policy->getTableName();
                        const String policy_name = policy->getShortName();
                        database_column->insertData(database.data(), database.length());
                        table_name_column->insertData(table_name.data(), table_name.length());
                        policy_name_column->insertData(policy_name.data(), policy_name.length());
                    }
                }
            }
            auto offset_column = ColumnArray::ColumnOffsets::create();
            offset_column->insertValue(policy_name_column->size());
            block.getByPosition(result_pos).column = ColumnConst::create(
                ColumnArray::create(
                    ColumnTuple::create(Columns{std::move(database_column), std::move(table_name_column), std::move(policy_name_column)}),
                    std::move(offset_column)),
                input_rows_count);
            return;
        }

        const IColumn * database_column = nullptr;
        if (arguments.size() == 2)
        {
            const auto & database_column_with_type = block.getByPosition(arguments[0]);
            if (!isStringOrFixedString(database_column_with_type.type))
                throw Exception{"The first argument of function " + String(name)
                                    + " should be a string containing database name, illegal type: "
                                    + database_column_with_type.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            database_column = database_column_with_type.column.get();
        }

        const auto & table_name_column_with_type = block.getByPosition(arguments[arguments.size() - 1]);
        if (!isStringOrFixedString(table_name_column_with_type.type))
            throw Exception{"The" + String(database_column ? " last" : "") + " argument of function " + String(name)
                                + " should be a string containing table name, illegal type: " + table_name_column_with_type.type->getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        const IColumn * table_name_column = table_name_column_with_type.column.get();

        auto policy_name_column = ColumnString::create();
        auto offset_column = ColumnArray::ColumnOffsets::create();
        for (const auto i : ext::range(0, input_rows_count))
        {
            String database = database_column ? database_column->getDataAt(i).toString() : context.getCurrentDatabase();
            String table_name = table_name_column->getDataAt(i).toString();
            if (auto policies = context.getRowPolicies())
            {
                for (const auto & policy_id : policies->getCurrentPolicyIDs(database, table_name))
                {
                    const auto policy = context.getAccessControlManager().tryRead<RowPolicy>(policy_id);
                    if (policy)
                    {
                        const String policy_name = policy->getShortName();
                        policy_name_column->insertData(policy_name.data(), policy_name.length());
                    }
                }
            }
            offset_column->insertValue(policy_name_column->size());
        }

        block.getByPosition(result_pos).column = ColumnArray::create(std::move(policy_name_column), std::move(offset_column));
    }

private:
    const Context & context;
};


/// The currentRowPolicyIDs() function can be called with 0..2 arguments:
/// currentRowPolicyIDs() returns array of IDs of all the row policies applied for the current user;
/// currentRowPolicyIDs(table_name) is equivalent to currentRowPolicyIDs(currentDatabase(), table_name);
/// currentRowPolicyIDs(database, table_name) returns array of IDs of the row policies applied to a specific table and for the current user.
class FunctionCurrentRowPolicyIDs : public IFunction
{
public:
    static constexpr auto name = "currentRowPolicyIDs";

    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionCurrentRowPolicyIDs>(context_); }
    explicit FunctionCurrentRowPolicyIDs(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    void checkNumberOfArgumentsIfVariadic(size_t number_of_arguments) const override
    {
        if (number_of_arguments > 2)
            throw Exception("Number of arguments for function " + String(name) + " doesn't match: passed "
                            + toString(number_of_arguments) + ", should be 0..2",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /* arguments */) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUUID>());
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_pos, size_t input_rows_count) override
    {
        if (arguments.empty())
        {
            auto policy_id_column = ColumnVector<UInt128>::create();
            if (auto policies = context.getRowPolicies())
            {
                for (const auto & policy_id : policies->getCurrentPolicyIDs())
                     policy_id_column->insertValue(policy_id);
            }
            auto offset_column = ColumnArray::ColumnOffsets::create();
            offset_column->insertValue(policy_id_column->size());
            block.getByPosition(result_pos).column
                = ColumnConst::create(ColumnArray::create(std::move(policy_id_column), std::move(offset_column)), input_rows_count);
            return;
        }

        const IColumn * database_column = nullptr;
        if (arguments.size() == 2)
        {
            const auto & database_column_with_type = block.getByPosition(arguments[0]);
            if (!isStringOrFixedString(database_column_with_type.type))
                throw Exception{"The first argument of function " + String(name)
                                    + " should be a string containing database name, illegal type: "
                                    + database_column_with_type.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            database_column = database_column_with_type.column.get();
        }

        const auto & table_name_column_with_type = block.getByPosition(arguments[arguments.size() - 1]);
        if (!isStringOrFixedString(table_name_column_with_type.type))
            throw Exception{"The" + String(database_column ? " last" : "") + " argument of function " + String(name)
                                + " should be a string containing table name, illegal type: " + table_name_column_with_type.type->getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        const IColumn * table_name_column = table_name_column_with_type.column.get();

        auto policy_id_column = ColumnVector<UInt128>::create();
        auto offset_column = ColumnArray::ColumnOffsets::create();
        for (const auto i : ext::range(0, input_rows_count))
        {
            String database = database_column ? database_column->getDataAt(i).toString() : context.getCurrentDatabase();
            String table_name = table_name_column->getDataAt(i).toString();
            if (auto policies = context.getRowPolicies())
            {
                for (const auto & policy_id : policies->getCurrentPolicyIDs(database, table_name))
                    policy_id_column->insertValue(policy_id);
            }
            offset_column->insertValue(policy_id_column->size());
        }

        block.getByPosition(result_pos).column = ColumnArray::create(std::move(policy_id_column), std::move(offset_column));
    }

private:
    const Context & context;
};


void registerFunctionCurrentRowPolicies(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentRowPolicies>();
    factory.registerFunction<FunctionCurrentRowPolicyIDs>();
}

}
