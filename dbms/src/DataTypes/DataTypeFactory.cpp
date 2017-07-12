#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTEnumElement.h>
#include <Parsers/ParserEnumElement.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypeEnum.h>

#include <ext/map.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int UNKNOWN_TYPE;
    extern const int NESTED_TYPE_TOO_DEEP;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


DataTypeFactory::DataTypeFactory()
    : non_parametric_data_types
    {
        {"UInt8",    std::make_shared<DataTypeUInt8>()},
        {"UInt16",   std::make_shared<DataTypeUInt16>()},
        {"UInt32",   std::make_shared<DataTypeUInt32>()},
        {"UInt64",   std::make_shared<DataTypeUInt64>()},
        {"Int8",     std::make_shared<DataTypeInt8>()},
        {"Int16",    std::make_shared<DataTypeInt16>()},
        {"Int32",    std::make_shared<DataTypeInt32>()},
        {"Int64",    std::make_shared<DataTypeInt64>()},
        {"Float32",  std::make_shared<DataTypeFloat32>()},
        {"Float64",  std::make_shared<DataTypeFloat64>()},
        {"Date",     std::make_shared<DataTypeDate>()},
        {"DateTime", std::make_shared<DataTypeDateTime>()},
        {"UUID",     std::make_shared<DataTypeUUID>()},
        {"String",   std::make_shared<DataTypeString>()},
        {"Null",     std::make_shared<DataTypeNull>()}
    }
{
}


template <typename DataTypeEnum>
inline DataTypePtr parseEnum(const String & name, const String & base_name, const String & parameters)
{
    ParserList parser{std::make_unique<ParserEnumElement>(), std::make_unique<ParserString>(","), false};

    ASTPtr elements = parseQuery(parser, parameters.data(), parameters.data() + parameters.size(), "parameters for enum type " + name);

    typename DataTypeEnum::Values values;
    values.reserve(elements->children.size());

    using FieldType = typename DataTypeEnum::FieldType;

    for (const auto & element : typeid_cast<const ASTExpressionList &>(*elements).children)
    {
        const auto & e = static_cast<const ASTEnumElement &>(*element);
        const auto value = e.value.get<typename NearestFieldType<FieldType>::Type>();

        if (value > std::numeric_limits<FieldType>::max() || value < std::numeric_limits<FieldType>::min())
            throw Exception{
                "Value " + applyVisitor(FieldVisitorToString{}, e.value) + " for element '" + e.name + "' exceeds range of " + base_name,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND
            };

        values.emplace_back(e.name, value);
    }

    return std::make_shared<DataTypeEnum>(values);
}


DataTypePtr DataTypeFactory::get(const String & name) const
{
    return getImpl(name, true);
}


DataTypePtr DataTypeFactory::getImpl(const String & name, bool allow_nullable) const
{
    NonParametricDataTypes::const_iterator it = non_parametric_data_types.find(name);
    if (it != non_parametric_data_types.end())
        return it->second;

    Poco::RegularExpression::MatchVec matches;
    if (fixed_string_regexp.match(name, 0, matches) && matches.size() == 2)
        return std::make_shared<DataTypeFixedString>(parse<size_t>(name.data() + matches[1].offset, matches[1].length));

    if (nested_regexp.match(name, 0, matches) && matches.size() == 3)
    {
        String base_name(name.data() + matches[1].offset, matches[1].length);
        String parameters(name.data() + matches[2].offset, matches[2].length);

        if (base_name == "Nullable")
        {
            if (!allow_nullable)
                throw Exception{"A Nullable type cannot contain another Nullable type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            return std::make_shared<DataTypeNullable>(getImpl(parameters, false));
        }

        if (base_name == "Array")
        {
            if (parameters == "Null")
            {
                /// Special case: Array(Null) is actually Array(Nullable(UInt8)).
                return std::make_shared<DataTypeArray>(
                    std::make_shared<DataTypeNullable>(
                        std::make_shared<DataTypeUInt8>()));
            }
            else
                return std::make_shared<DataTypeArray>(getImpl(parameters, allow_nullable));
        }

        if (base_name == "AggregateFunction")
        {
            String function_name;
            AggregateFunctionPtr function;
            DataTypes argument_types;
            Array params_row;

            ParserExpressionList args_parser(false);
            ASTPtr args_ast = parseQuery(args_parser, parameters.data(), parameters.data() + parameters.size(), "parameters for data type " + name);
            ASTExpressionList & args_list = typeid_cast<ASTExpressionList &>(*args_ast);

            if (args_list.children.empty())
                throw Exception("Data type AggregateFunction requires parameters: "
                    "name of aggregate function and list of data types for arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (ASTFunction * parametric = typeid_cast<ASTFunction *>(args_list.children[0].get()))
            {
                if (parametric->parameters)
                    throw Exception("Unexpected level of parameters to aggregate function", ErrorCodes::SYNTAX_ERROR);
                function_name = parametric->name;

                ASTs & parameters = typeid_cast<ASTExpressionList &>(*parametric->arguments).children;
                params_row.resize(parameters.size());

                for (size_t i = 0; i < parameters.size(); ++i)
                {
                    ASTLiteral * lit = typeid_cast<ASTLiteral *>(parameters[i].get());
                    if (!lit)
                        throw Exception("Parameters to aggregate functions must be literals",
                            ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);

                    params_row[i] = lit->value;
                }
            }
            else if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(args_list.children[0].get()))
            {
                function_name = identifier->name;
            }
            else if (typeid_cast<ASTLiteral *>(args_list.children[0].get()))
            {
                throw Exception("Aggregate function name for data type AggregateFunction must be passed as identifier (without quotes) or function",
                    ErrorCodes::BAD_ARGUMENTS);
            }
            else
                throw Exception("Unexpected AST element passed as aggregate function name for data type AggregateFunction. Must be identifier or function.",
                    ErrorCodes::BAD_ARGUMENTS);

            for (size_t i = 1; i < args_list.children.size(); ++i)
                argument_types.push_back(getImpl(
                    std::string{args_list.children[i]->range.first, args_list.children[i]->range.second}, allow_nullable));

            if (function_name.empty())
                throw Exception("Logical error: empty name of aggregate function passed", ErrorCodes::LOGICAL_ERROR);

            function = AggregateFunctionFactory::instance().get(function_name, argument_types);
            if (!params_row.empty())
                function->setParameters(params_row);
            function->setArguments(argument_types);
            return std::make_shared<DataTypeAggregateFunction>(function, argument_types, params_row);
        }

        if (base_name == "Nested")
        {
            ParserNameTypePairList columns_p;
            ASTPtr columns_ast = parseQuery(columns_p, parameters.data(), parameters.data() + parameters.size(), "parameters for data type " + name);

            NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();

            ASTExpressionList & columns_list = typeid_cast<ASTExpressionList &>(*columns_ast);
            for (ASTs::iterator it = columns_list.children.begin(); it != columns_list.children.end(); ++it)
            {
                ASTNameTypePair & name_and_type_pair = typeid_cast<ASTNameTypePair &>(**it);
                StringRange type_range = name_and_type_pair.type->range;
                DataTypePtr type = getImpl(String(type_range.first, type_range.second - type_range.first), allow_nullable);
                if (typeid_cast<const DataTypeNested *>(type.get()))
                    throw Exception("Nested inside Nested is not allowed", ErrorCodes::NESTED_TYPE_TOO_DEEP);
                columns->push_back(NameAndTypePair(
                    name_and_type_pair.name,
                    type));
            }

            return std::make_shared<DataTypeNested>(columns);
        }

        if (base_name == "Tuple")
        {
            ParserExpressionList columns_p(false);
            ASTPtr columns_ast = parseQuery(columns_p, parameters.data(), parameters.data() + parameters.size(), "parameters for data type " + name);

            auto & columns_list = typeid_cast<ASTExpressionList &>(*columns_ast);
            const auto elems = ext::map<DataTypes>(columns_list.children, [&] (const ASTPtr & elem_ast) {
                return getImpl(String(elem_ast->range.first, elem_ast->range.second), allow_nullable);
            });

            return std::make_shared<DataTypeTuple>(elems);
        }

        if (base_name == "Enum8")
            return parseEnum<DataTypeEnum8>(name, base_name, parameters);

        if (base_name == "Enum16")
            return parseEnum<DataTypeEnum16>(name, base_name, parameters);

        throw Exception("Unknown type " + base_name, ErrorCodes::UNKNOWN_TYPE);
    }

    throw Exception("Unknown type " + name, ErrorCodes::UNKNOWN_TYPE);
}


}
