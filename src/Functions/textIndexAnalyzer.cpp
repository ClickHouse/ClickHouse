#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/MergeTree/MergeTreeTextIndexAnalyzer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
}

namespace
{

/// Internal column name used when building ExpressionActions for the preprocessor.
constexpr const char * INTERNAL_COLUMN_NAME = "__text_index_analyzer_input";

/// Replaces all occurrences of "{input}" in the definition string with an internal column name
/// so that the SQL parser sees a valid identifier.
String substituteInputPlaceholder(const String & definition)
{
    static constexpr std::string_view placeholder = "{input}";

    String result;
    result.reserve(definition.size());

    size_t pos = 0;
    while (pos < definition.size())
    {
        size_t found = definition.find(placeholder, pos);
        if (found == String::npos)
        {
            result.append(definition, pos);
            break;
        }
        result.append(definition, pos, found - pos);
        result.append(INTERNAL_COLUMN_NAME);
        pos = found + placeholder.size();
    }

    return result;
}

/// Builds ExpressionActions from a preprocessor AST like lower(__text_index_analyzer_input).
ExpressionActionsPtr buildPreprocessorActions(const ASTPtr & preprocessor_ast)
{
    if (!preprocessor_ast)
        return nullptr;

    ASTPtr expr = preprocessor_ast->clone();

    auto context = Context::getGlobalContextInstance();
    NamesAndTypesList source_columns = {{INTERNAL_COLUMN_NAME, std::make_shared<DataTypeString>()}};

    auto syntax_result = TreeRewriter(context).analyze(expr, source_columns);
    auto actions_dag = ExpressionAnalyzer(expr, syntax_result, context).getActionsDAG(false, true);

    auto expression_name = expr->getColumnName();
    actions_dag.project({{expression_name, expression_name}});
    actions_dag.removeUnusedActions();

    const auto & outputs = actions_dag.getOutputs();
    if (outputs.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must return a single column, but got {} output columns", outputs.size());

    if (outputs.front()->type != ActionsDAG::ActionType::FUNCTION)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must be a function applied to '{{input}}', but got '{}' action type",
            outputs.front()->type);

    auto output_type = outputs.front()->result_type;
    WhichDataType which(output_type);
    if (!which.isString() && !which.isFixedString())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must return String or FixedString, but got: {}", output_type->getName());

    return std::make_shared<ExpressionActions>(std::move(actions_dag));
}

/// Applies preprocessor ExpressionActions to an input column.
ColumnPtr applyPreprocessor(const ExpressionActions & actions, ColumnPtr col_input, size_t input_rows_count)
{
    Block block{{col_input, std::make_shared<DataTypeString>(), INTERNAL_COLUMN_NAME}};
    actions.execute(block, input_rows_count);
    return block.safeGetByPosition(0).column;
}

class ExecutableFunctionTextIndexAnalyzer : public IExecutableFunction
{
public:
    static constexpr auto name = "__textIndexAnalyzer";

    ExecutableFunctionTextIndexAnalyzer(std::shared_ptr<const ITokenizer> tokenizer_, ExpressionActionsPtr preprocessor_)
        : tokenizer(std::move(tokenizer_))
        , preprocessor(std::move(preprocessor_))
    {
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_input = arguments[1].column;
        auto col_result = ColumnString::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        if (input_rows_count == 0)
            return ColumnArray::create(std::move(col_result), std::move(col_offsets));

        if (preprocessor)
            col_input = applyPreprocessor(*preprocessor, std::move(col_input), input_rows_count);

        if (tokenizer->getType() == ITokenizer::Type::SparseGrams)
        {
            auto sparse_grams_tokenizer = tokenizer->clone();
            executeWithTokenizer(*sparse_grams_tokenizer, std::move(col_input), *col_offsets, input_rows_count, *col_result);
        }
        else
        {
            executeWithTokenizer(*tokenizer, std::move(col_input), *col_offsets, input_rows_count, *col_result);
        }

        return ColumnArray::create(std::move(col_result), std::move(col_offsets));
    }

private:
    void executeWithTokenizer(
        const ITokenizer & tokenizer_,
        ColumnPtr col_input,
        ColumnArray::ColumnOffsets & col_offsets,
        size_t input_rows_count,
        ColumnString & col_result) const
    {
        if (const auto * column_string = checkAndGetColumn<ColumnString>(col_input.get()))
            executeImpl(tokenizer_, *column_string, col_offsets, input_rows_count, col_result);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input.get()))
            executeImpl(tokenizer_, *column_fixed_string, col_offsets, input_rows_count, col_result);
    }

    template <typename StringColumnType>
    void executeImpl(
        const ITokenizer & tokenizer_,
        const StringColumnType & column_input,
        ColumnArray::ColumnOffsets & column_offsets_input,
        size_t input_rows_count,
        ColumnString & column_result) const
    {
        auto & offsets_data = column_offsets_input.getData();
        offsets_data.resize(input_rows_count);
        size_t tokens_count = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view input = column_input.getDataAt(i);

            forEachToken(
                tokenizer_,
                input.data(),
                input.size(),
                [&](const char * token_start, size_t token_len)
                {
                    column_result.insertData(token_start, token_len);
                    ++tokens_count;
                    return false;
                });

            offsets_data[i] = tokens_count;
        }
    }

    std::shared_ptr<const ITokenizer> tokenizer;
    ExpressionActionsPtr preprocessor;
};

class FunctionBaseTextIndexAnalyzer : public IFunctionBase
{
public:
    static constexpr auto name = "__textIndexAnalyzer";

    FunctionBaseTextIndexAnalyzer(
        std::shared_ptr<const ITokenizer> tokenizer_,
        ExpressionActionsPtr preprocessor_,
        DataTypes argument_types_,
        DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
        , preprocessor(std::move(preprocessor_))
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
    {
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionTextIndexAnalyzer>(tokenizer, preprocessor);
    }

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    ExpressionActionsPtr preprocessor;
    DataTypes argument_types;
    DataTypePtr result_type;
};

class FunctionTextIndexAnalyzerOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "__textIndexAnalyzer";

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<FunctionTextIndexAnalyzerOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"definition", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
        };

        validateFunctionArguments(name, arguments, mandatory_args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (!arguments[0].column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument of function {} must be a constant string", name);

        String definition_str(arguments[0].column->getDataAt(0));
        definition_str = substituteInputPlaceholder(definition_str);

        auto options = textIndexParseConfigString(definition_str);
        auto params = textIndexValidateOptions(options);

        auto tokenizer = TokenizerFactory::instance().get(params.tokenizer);
        auto preprocessor_actions = buildPreprocessorActions(params.preprocessor);

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            argument_types.push_back(arg.type);

        return std::make_shared<FunctionBaseTextIndexAnalyzer>(
            std::move(tokenizer), std::move(preprocessor_actions), std::move(argument_types), return_type);
    }
};

}

REGISTER_FUNCTION(TextIndexAnalyzer)
{
    factory.registerFunction<FunctionTextIndexAnalyzerOverloadResolver>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}
}
