#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
    extern const int SYNTAX_ERROR;
}

struct LiteralInfo
{
    typedef std::shared_ptr<ASTLiteral> ASTLiteralPtr;
    LiteralInfo(const ASTLiteralPtr & literal_, const String & column_name_, bool force_nullable_)
            : literal(literal_), dummy_column_name(column_name_), force_nullable(force_nullable_) { }
    ASTLiteralPtr literal;
    String dummy_column_name;
    /// Make column nullable even if expression type is not.
    /// (for literals in functions like ifNull and assumeNotNul, which never return NULL even for NULL arguments)
    bool force_nullable;
};

using LiteralsInfo = std::vector<LiteralInfo>;

class ReplaceLiteralsVisitor
{
public:
    LiteralsInfo replaced_literals;
    const Context & context;

    explicit ReplaceLiteralsVisitor(const Context & context_) : context(context_) { }

    void visit(ASTPtr & ast, bool force_nullable = {})
    {
        if (visitIfLiteral(ast, force_nullable))
            return;
        if (auto function = ast->as<ASTFunction>())
            visit(*function, force_nullable);
        else if (ast->as<ASTLiteral>())
            throw DB::Exception("Identifier in constant expression", ErrorCodes::SYNTAX_ERROR);
        else
            visitChildren(ast, {}, std::vector<char>(ast->children.size(), force_nullable));
    }

private:
    void visitChildren(ASTPtr & ast, const ColumnNumbers & dont_visit_children, const std::vector<char> & force_nullable)
    {
        for (size_t i = 0; i < ast->children.size(); ++i)
            if (std::find(dont_visit_children.begin(), dont_visit_children.end(), i) == dont_visit_children.end())
                visit(ast->children[i], force_nullable[i]);
    }

    void visit(ASTFunction & function, bool force_nullable)
    {
        /// Do not replace literals which must be constant
        ColumnNumbers dont_visit_children;
        FunctionBuilderPtr builder = FunctionFactory::instance().get(function.name, context);

        if (auto default_builder = dynamic_cast<DefaultFunctionBuilder*>(builder.get()))
            dont_visit_children = default_builder->getArgumentsThatAreAlwaysConstant();
        else if (dynamic_cast<FunctionBuilderCast*>(builder.get()))
            dont_visit_children.push_back(1);
        /// FIXME suppose there is no other functions, which require constant arguments (it's true, until the new one is added)

        /// Allow nullable arguments if function never returns NULL
        bool return_not_null = function.name == "isNull" || function.name == "isNotNull" ||
                               function.name == "ifNull"  || function.name == "assumeNotNull" ||
                               function.name == "coalesce";

        std::vector<char> force_nullable_arguments(function.arguments->children.size(), force_nullable || return_not_null);

        /// coalesce may return NULL if the last argument is NULL
        if (!force_nullable && function.name == "coalesce")
            force_nullable_arguments.back() = false;

        visitChildren(function.arguments, dont_visit_children, force_nullable_arguments);
    }

    bool visitIfLiteral(ASTPtr & ast, bool force_nullable)
    {
        auto literal = std::dynamic_pointer_cast<ASTLiteral>(ast);
        if (!literal)
            return false;
        if (literal->begin && literal->end)
        {
            /// Do not replace empty array
            if (literal->value.getType() == Field::Types::Array && literal->value.get<Array>().empty())
                return false;
            String column_name = "_dummy_" + std::to_string(replaced_literals.size());
            replaced_literals.emplace_back(literal, column_name, force_nullable);
            ast = std::make_shared<ASTIdentifier>(column_name);
        }
        return true;
    }
};


/// Expression template is a sequence of tokens and data types of literals.
/// E.g. template of "position('some string', 'other string') != 0" is
/// ["position", "(", DataTypeString, ",", DataTypeString, ")", "!=", DataTypeUInt64]
ConstantExpressionTemplate::ConstantExpressionTemplate(DataTypePtr result_column_type_,
                                                       TokenIterator expression_begin, TokenIterator expression_end,
                                                       const ASTPtr & expression_, const Context & context)
                                                       : result_column_type(result_column_type_)
{
    /// Extract ASTLiterals from expression and replace them with ASTIdentifiers where needed
    ASTPtr expression = expression_->clone();
    ReplaceLiteralsVisitor visitor(context);
    visitor.visit(expression);
    LiteralsInfo & replaced_literals = visitor.replaced_literals;

    std::sort(replaced_literals.begin(), replaced_literals.end(), [](const LiteralInfo & a, const LiteralInfo & b)
    {
        return a.literal->begin.value() < b.literal->begin.value();
    });

    /// Make sequence of tokens and determine IDataType by Field::Types:Which for each literal.
    token_after_literal_idx.reserve(replaced_literals.size());
    use_special_parser.resize(replaced_literals.size(), true);

    TokenIterator prev_end = expression_begin;
    for (size_t i = 0; i < replaced_literals.size(); ++i)
    {
        const LiteralInfo & info = replaced_literals[i];
        if (info.literal->begin.value() < prev_end)
            throw Exception("Cannot replace literals", ErrorCodes::LOGICAL_ERROR);

        while (prev_end < info.literal->begin.value())
        {
            tokens.emplace_back(prev_end->begin, prev_end->size());
            ++prev_end;
        }
        token_after_literal_idx.push_back(tokens.size());

        DataTypePtr type;
        use_special_parser[i] = getDataType(info, type);

        literals.insert({nullptr, type, info.dummy_column_name});

        prev_end = info.literal->end.value();
    }

    while (prev_end < expression_end)
    {
        tokens.emplace_back(prev_end->begin, prev_end->size());
        ++prev_end;
    }

    columns = literals.cloneEmptyColumns();
    addNodesToCastResult(*result_column_type, expression);
    result_column_name = expression->getColumnName();

    auto syntax_result = SyntaxAnalyzer(context).analyze(expression, literals.getNamesAndTypesList());
    actions_on_literals = ExpressionAnalyzer(expression, syntax_result, context).getActions(false);
}

bool ConstantExpressionTemplate::getDataType(const LiteralInfo & info, DataTypePtr & type) const
{
    /// Type (Field::Types:Which) of literal in AST can be: String, UInt64, Int64, Float64, Null or Array of simple literals (not of Arrays).
    /// Null and empty Array literals are considered as tokens, because template with Nullable<Nothing> or Array<Nothing> is useless.

    Field::Types::Which field_type = info.literal->value.getType();

    /// We have to use ParserNumber instead of type->deserializeAsTextQuoted() for arithmetic types
    /// to check actual type of literal and avoid possible overflow and precision issues.
    bool need_special_parser = true;

    /// Do not use 8, 16 and 32 bit types, so template will match all integers
    if (field_type == Field::Types::UInt64)
        type = std::make_shared<DataTypeUInt64>();
    else if (field_type == Field::Types::Int64)
        type = std::make_shared<DataTypeInt64>();
    else if (field_type == Field::Types::Float64)
        type = std::make_shared<DataTypeFloat64>();
    else if (field_type == Field::Types::String)
    {
        need_special_parser = false;
        type = std::make_shared<DataTypeString>();
    }
    else if (field_type == Field::Types::Array)
    {
        type = applyVisitor(FieldToDataType(), info.literal->value);
        auto nested_type = dynamic_cast<const DataTypeArray &>(*type).getNestedType();

        /// It can be Array<Nullable<nested_type>>
        bool array_of_nullable = false;
        if (auto nullable = dynamic_cast<const DataTypeNullable *>(type.get()))
        {
            nested_type = nullable->getNestedType();
            array_of_nullable = true;
        }

        WhichDataType type_info{nested_type};
        /// Promote integers to 64 bit types
        if (type_info.isNativeUInt())
            nested_type = std::make_shared<DataTypeUInt64>();
        else if (type_info.isNativeInt())
            nested_type = std::make_shared<DataTypeInt64>();
        else if (type_info.isFloat64())
            ;
        else if (type_info.isString())
            need_special_parser = false;
        else
            throw Exception("Unexpected literal type inside Array: " + nested_type->getName() + ". It's a bug",
                            ErrorCodes::LOGICAL_ERROR);

        if (array_of_nullable)
            nested_type = std::make_shared<DataTypeNullable>(nested_type);

        type = std::make_shared<DataTypeArray>(nested_type);
    }
    else
        throw Exception(String("Unexpected literal type ") + info.literal->value.getTypeName() + ". It's a bug",
                        ErrorCodes::LOGICAL_ERROR);

    /// Allow literal to be NULL, if result column has nullable type or if function never returns NULL
    bool allow_nulls = result_column_type->isNullable();
    if ((allow_nulls || info.force_nullable) && type->canBeInsideNullable())
        type = makeNullable(type);

    return need_special_parser;
}

void ConstantExpressionTemplate::parseExpression(ReadBuffer & istr, const FormatSettings & settings)
{
    size_t cur_column = 0;
    try
    {
        size_t cur_token = 0;
        while (cur_column < literals.columns())
        {
            size_t skip_tokens_until = token_after_literal_idx[cur_column];
            while (cur_token < skip_tokens_until)
            {
                /// TODO skip comments
                skipWhitespaceIfAny(istr);
                assertString(tokens[cur_token++], istr);
            }
            skipWhitespaceIfAny(istr);

            const IDataType & type = *literals.getByPosition(cur_column).type;
            if (use_special_parser[cur_column])
                parseLiteralAndAssertType(istr, type, cur_column);
            else
                type.deserializeAsTextQuoted(*columns[cur_column], istr, settings);

            ++cur_column;
        }
        while (cur_token < tokens.size())
        {
            skipWhitespaceIfAny(istr);
            assertString(tokens[cur_token++], istr);
        }
        ++rows_count;
    }
    catch (DB::Exception & e)
    {
        for (size_t i = 0; i < cur_column; ++i)
            columns[i]->popBack(1);

        if (!isParseError(e.code()))
            throw;

        throw DB::Exception("Cannot parse expression using template", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
    }
}

void ConstantExpressionTemplate::parseLiteralAndAssertType(ReadBuffer & istr, const IDataType & type, size_t column_idx)
{
    /// TODO faster way to check types without using Parsers
    ParserKeyword parser_null("NULL");
    ParserNumber parser_number;
    ParserArrayOfLiterals parser_array;

    WhichDataType type_info(type);

    bool is_array = type_info.isArray();
    if (is_array)
        type_info = WhichDataType(dynamic_cast<const DataTypeArray &>(type).getNestedType());

    bool is_nullable = type_info.isNullable();
    if (is_nullable)
        type_info = WhichDataType(dynamic_cast<const DataTypeNullable &>(type).getNestedType());

    /// If literal does not fit entirely in the buffer, parsing error will happen.
    /// However, it's possible to deduce new template after error like it was template mismatch.
    /// TODO fix it
    Tokens tokens_number(istr.position(), istr.buffer().end());
    IParser::Pos iterator(tokens_number);
    Expected expected;
    ASTPtr ast;

    if (is_array)
    {
        if (!parser_array.parse(iterator, ast, expected))
            throw DB::Exception("Cannot parse literal", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
        istr.position() = const_cast<char *>(iterator->begin);

        const Field & array = ast->as<ASTLiteral &>().value;
        auto array_type = applyVisitor(FieldToDataType(), array);
        auto nested_type = dynamic_cast<const DataTypeArray &>(*array_type).getNestedType();
        if (is_nullable)
            if (auto nullable = dynamic_cast<const DataTypeNullable *>(nested_type.get()))
                nested_type = nullable->getNestedType();

        WhichDataType nested_type_info(nested_type);
        if ((nested_type_info.isNativeUInt() && type_info.isUInt64()) ||
            (nested_type_info.isNativeInt()  && type_info.isInt64())  ||
            (nested_type_info.isFloat64()    && type_info.isFloat64()))
        {
            columns[column_idx]->insert(array);
            return;
        }
    }
    else
    {
        if (is_nullable && parser_null.parse(iterator, ast, expected))
            ast = std::make_shared<ASTLiteral>(Field());
        else if (!parser_number.parse(iterator, ast, expected))
            throw DB::Exception("Cannot parse literal", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
        istr.position() = const_cast<char *>(iterator->begin);

        Field & number = ast->as<ASTLiteral &>().value;

        if ((number.getType() == Field::Types::UInt64  && type_info.isUInt64())  ||
            (number.getType() == Field::Types::Int64   && type_info.isInt64())   ||
            (number.getType() == Field::Types::Float64 && type_info.isFloat64()) ||
            is_nullable)
        {
            columns[column_idx]->insert(number);
            return;
        }
    }
    throw DB::Exception("Cannot parse literal: type mismatch", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
}

ColumnPtr ConstantExpressionTemplate::evaluateAll()
{
    Block evaluated = literals.cloneWithColumns(std::move(columns));
    columns = literals.cloneEmptyColumns();
    if (!literals.columns())
        evaluated.insert({ColumnConst::create(ColumnUInt8::create(1, 0), rows_count), std::make_shared<DataTypeUInt8>(), "_dummy"});
    actions_on_literals->execute(evaluated);

    if (!evaluated || evaluated.rows() != rows_count)
        throw Exception("Number of rows mismatch after evaluation of batch of constant expressions: got " +
                        std::to_string(evaluated.rows()) + " rows for " + std::to_string(rows_count) + " expressions",
                        ErrorCodes::LOGICAL_ERROR);

    if (!evaluated.has(result_column_name))
        throw Exception("Cannot evaluate template " + result_column_name + ", block structure:\n" + evaluated.dumpStructure(),
                        ErrorCodes::LOGICAL_ERROR);

    rows_count = 0;
    return evaluated.getByName(result_column_name).column->convertToFullColumnIfConst();
}


void ConstantExpressionTemplate::addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr)
{
    auto result_type = std::make_shared<ASTLiteral>(result_column_type.getName());

    auto arguments = std::make_shared<ASTExpressionList>();
    arguments->children.push_back(std::move(expr));
    arguments->children.push_back(std::move(result_type));

    auto cast = std::make_shared<ASTFunction>();
    cast->name = "CAST";
    cast->arguments = std::move(arguments);
    cast->children.push_back(cast->arguments);

    expr = std::move(cast);
}

}
