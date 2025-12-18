#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/GraphQLParsers/GraphQLParser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// GraphQLExtractString(query, field_path) -> String
/// Extracts a field name from a GraphQL query by its dot-separated path.
/// Example: GraphQLExtractString('{ user { name } }', 'user.name') -> 'name'
class FunctionGraphQLExtractString : public IFunction
{
public:
    static constexpr auto name = "GraphQLExtractString";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGraphQLExtractString>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String (GraphQL query)",
                arguments[0]->getName(),
                getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String (field path)",
                arguments[1]->getName(),
                getName());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * query_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        const ColumnString * path_col = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        if (!query_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be String", getName());
        if (!path_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be String", getName());

        auto result_col = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        result_col->reserve(input_rows_count);

        GraphQLParser parser;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query = query_col->getDataAt(i).toView();
            std::string_view path = path_col->getDataAt(i).toView();

            bool found = false;
            if (parser.parse(query))
            {
                std::string result = parser.extractField(std::string(path));
                if (!result.empty())
                {
                    result_col->insertData(result.data(), result.size());
                    found = true;
                }
            }

            if (!found)
            {
                result_col->insertDefault();
                null_map->getData()[i] = 1;
            }
        }

        return ColumnNullable::create(std::move(result_col), std::move(null_map));
    }
};


/// GraphQLExtractVariable(query, variable_name) -> String
/// Extracts a variable definition from a GraphQL query.
/// Returns the default value if specified, otherwise the type.
class FunctionGraphQLExtractVariable : public IFunction
{
public:
    static constexpr auto name = "GraphQLExtractVariable";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGraphQLExtractVariable>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String (GraphQL query)",
                arguments[0]->getName(),
                getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String (variable name)",
                arguments[1]->getName(),
                getName());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * query_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        const ColumnString * name_col = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        if (!query_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be String", getName());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be String", getName());

        auto result_col = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        result_col->reserve(input_rows_count);

        GraphQLParser parser;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query = query_col->getDataAt(i).toView();
            std::string_view var_name = name_col->getDataAt(i).toView();

            bool found = false;
            if (parser.parse(query))
            {
                std::string result = parser.extractVariable(std::string(var_name));
                if (!result.empty())
                {
                    result_col->insertData(result.data(), result.size());
                    found = true;
                }
            }

            if (!found)
            {
                result_col->insertDefault();
                null_map->getData()[i] = 1;
            }
        }

        return ColumnNullable::create(std::move(result_col), std::move(null_map));
    }
};


/// GraphQLGetOperationType(query) -> String
/// Returns the operation type (query, mutation, subscription) of a GraphQL query.
class FunctionGraphQLGetOperationType : public IFunction
{
public:
    static constexpr auto name = "GraphQLGetOperationType";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGraphQLGetOperationType>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected String (GraphQL query)",
                arguments[0]->getName(),
                getName());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * query_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());

        if (!query_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument for function {} must be String", getName());

        auto result_col = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        result_col->reserve(input_rows_count);

        GraphQLParser parser;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view query = query_col->getDataAt(i).toView();

            bool found = false;
            if (parser.parse(query))
            {
                std::string result = parser.getOperationType();
                if (!result.empty())
                {
                    result_col->insertData(result.data(), result.size());
                    found = true;
                }
            }

            if (!found)
            {
                result_col->insertDefault();
                null_map->getData()[i] = 1;
            }
        }

        return ColumnNullable::create(std::move(result_col), std::move(null_map));
    }
};

}

REGISTER_FUNCTION(GraphQL)
{
    /// GraphQLExtractString
    {
        FunctionDocumentation::Description description = R"(
Extracts a field name from a GraphQL query by its dot-separated path.

The path uses dot notation to navigate through the query structure.
Start with the operation type (query, mutation, subscription) or directly with field names.

Examples:
- 'user.name' - extracts 'name' from the user field
- 'query.user.email' - explicitly starts from query operation
- 'mutation.createUser.id' - navigates mutation operation
)";
        FunctionDocumentation::Syntax syntax = "GraphQLExtractString(query, field_path)";
        FunctionDocumentation::Arguments arguments = {
            {"query", "GraphQL query string", {"String"}},
            {"field_path", "Dot-separated path to the field", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "The field name at the specified path, or NULL if not found",
            {"Nullable(String)"}
        };
        FunctionDocumentation::Examples examples = {
            {"simple",
             "SELECT GraphQLExtractString('{ user { name email } }', 'user.name')",
             "name"},
            {"mutation",
             "SELECT GraphQLExtractString('mutation { createUser(input: {name: \"John\"}) { id } }', 'mutation.createUser.id')",
             "id"}
        };
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, {}, category};

        factory.registerFunction<FunctionGraphQLExtractString>(documentation);
    }

    /// GraphQLExtractVariable
    {
        FunctionDocumentation::Description description = R"(
Extracts a variable definition from a GraphQL query.

Returns the default value if specified in the query, otherwise returns the variable type.
Variable names should be specified without the $ prefix.
)";
        FunctionDocumentation::Syntax syntax = "GraphQLExtractVariable(query, variable_name)";
        FunctionDocumentation::Arguments arguments = {
            {"query", "GraphQL query string with variable definitions", {"String"}},
            {"variable_name", "Name of the variable (without $)", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "The variable's default value or type, or NULL if not found",
            {"Nullable(String)"}
        };
        FunctionDocumentation::Examples examples = {
            {"with_default",
             "SELECT GraphQLExtractVariable('query GetUser($id: ID! = \"123\") { user(id: $id) { name } }', 'id')",
             "123"},
            {"type_only",
             "SELECT GraphQLExtractVariable('query GetUser($id: ID!) { user(id: $id) { name } }', 'id')",
             "ID!"}
        };
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, {}, category};

        factory.registerFunction<FunctionGraphQLExtractVariable>(documentation);
    }

    /// GraphQLGetOperationType
    {
        FunctionDocumentation::Description description = R"(
Returns the operation type of a GraphQL query.

Possible return values: 'query', 'mutation', 'subscription'.
For anonymous queries (starting with {), returns 'query'.
)";
        FunctionDocumentation::Syntax syntax = "GraphQLGetOperationType(query)";
        FunctionDocumentation::Arguments arguments = {
            {"query", "GraphQL query string", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "The operation type, or NULL if parsing fails",
            {"Nullable(String)"}
        };
        FunctionDocumentation::Examples examples = {
            {"query",
             "SELECT GraphQLGetOperationType('query { user { name } }')",
             "query"},
            {"mutation",
             "SELECT GraphQLGetOperationType('mutation { createUser { id } }')",
             "mutation"},
            {"anonymous",
             "SELECT GraphQLGetOperationType('{ user { name } }')",
             "query"}
        };
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, {}, category};

        factory.registerFunction<FunctionGraphQLGetOperationType>(documentation);
    }
}

}
