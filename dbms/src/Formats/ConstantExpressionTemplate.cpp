
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
#include <Formats/ConstantExpressionTemplate.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_EXPRESSION_TEMPLATE;
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
    extern const int CANNOT_EVALUATE_EXPRESSION_TEMPLATE;
    extern const int SYNTAX_ERROR;
}

class ReplaceLiteralsVisitor
{
public:

    struct LiteralInfo
    {
        typedef std::shared_ptr<ASTLiteral> ASTLiteralPtr;
        LiteralInfo(const ASTLiteralPtr & literal_, const String & column_name_) : literal(literal_), dummy_column_name(column_name_) { }
        ASTLiteralPtr literal;
        String dummy_column_name;
    };

    using LiteralsInfo = std::vector<LiteralInfo>;

    LiteralsInfo replaced_literals;
    const Context & context;

    explicit ReplaceLiteralsVisitor(const Context & context_) : context(context_) { }

    void visit(ASTPtr & ast)
    {
        if (visitIfLiteral(ast))
            return;
        if (auto function = ast->as<ASTFunction>())
            visit(*function);
        else if (ast->as<ASTLiteral>())
            throw DB::Exception("Identifier in constant expression", ErrorCodes::SYNTAX_ERROR);
        else
            visitChildren(ast, {});
    }

private:
    void visitChildren(ASTPtr & ast, const ColumnNumbers & dont_visit_children)
    {
        for (size_t i = 0; i < ast->children.size(); ++i)
            if (std::find(dont_visit_children.begin(), dont_visit_children.end(), i) == dont_visit_children.end())
                visit(ast->children[i]);
    }

    void visit(ASTFunction & function)
    {
        /// Do not replace literals which must be constant
        ColumnNumbers dont_visit_children;
        FunctionBuilderPtr builder = FunctionFactory::instance().get(function.name, context);

        if (auto default_builder = dynamic_cast<DefaultFunctionBuilder*>(builder.get()))
            dont_visit_children = default_builder->getArgumentsThatAreAlwaysConstant();
        else if (dynamic_cast<FunctionBuilderCast*>(builder.get()))
            dont_visit_children.push_back(1);

        visitChildren(function.arguments, dont_visit_children);
    }

    bool visitIfLiteral(ASTPtr & ast)
    {
        auto literal = std::dynamic_pointer_cast<ASTLiteral>(ast);
        if (!literal)
            return false;
        if (literal->begin && literal->end)
        {
            String column_name = "_dummy_" + std::to_string(replaced_literals.size());
            replaced_literals.emplace_back(literal, column_name);
            ast = std::make_shared<ASTIdentifier>(column_name);
        }
        return true;
    }
};

using LiteralInfo = ReplaceLiteralsVisitor::LiteralInfo;
using LiteralsInfo = ReplaceLiteralsVisitor::LiteralsInfo;


ConstantExpressionTemplate::ConstantExpressionTemplate(const IDataType & result_column_type,
                                                       TokenIterator expression_begin, TokenIterator expression_end,
                                                       const ASTPtr & expression_, const Context & context)
{
    ASTPtr expression = expression_->clone();
    ReplaceLiteralsVisitor visitor(context);
    visitor.visit(expression);
    LiteralsInfo replaced_literals = visitor.replaced_literals;

    token_after_literal_idx.reserve(replaced_literals.size());
    need_special_parser.resize(replaced_literals.size(), true);

    std::sort(replaced_literals.begin(), replaced_literals.end(), [](const LiteralInfo & a, const LiteralInfo & b)
    {
        return a.literal->begin.value() < b.literal->begin.value();
    });

    bool allow_nulls = result_column_type.isNullable();
    TokenIterator prev_end = expression_begin;
    for (size_t i = 0; i < replaced_literals.size(); ++i)
    {
        const LiteralInfo & info = replaced_literals[i];
        if (info.literal->begin.value() < prev_end)
            throw Exception("Cannot replace literals", ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);

        while (prev_end < info.literal->begin.value())
        {
            tokens.emplace_back(prev_end->begin, prev_end->size());
            ++prev_end;
        }
        token_after_literal_idx.push_back(tokens.size());

        DataTypePtr type = applyVisitor(FieldToDataType(), info.literal->value);

        WhichDataType type_info(type);
        if (type_info.isNativeInt())
            type = std::make_shared<DataTypeInt64>();
        else if (type_info.isNativeUInt())
            type = std::make_shared<DataTypeUInt64>();
        else if (!type_info.isFloat())
            need_special_parser[i] = false;

        /// Allow literal to be NULL, if result column has nullable type
        // TODO also allow NULL literals inside functions, which return not NULL for NULL arguments,
        //  even if result_column_type is not nullable
        if (allow_nulls && type->canBeInsideNullable())
            type = makeNullable(type);

        literals.insert({nullptr, type, info.dummy_column_name});

        prev_end = info.literal->end.value();
    }

    while (prev_end < expression_end)
    {
        tokens.emplace_back(prev_end->begin, prev_end->size());
        ++prev_end;
    }

    columns = literals.cloneEmptyColumns();
    addNodesToCastResult(result_column_type, expression);
    result_column_name = expression->getColumnName();

    // TODO convert SyntaxAnalyzer and ExpressionAnalyzer exceptions to CANNOT_CREATE_EXPRESSION_TEMPLATE
    auto syntax_result = SyntaxAnalyzer(context).analyze(expression, literals.getNamesAndTypesList());

    actions_on_literals = ExpressionAnalyzer(expression, syntax_result, context).getActions(false);
}

void ConstantExpressionTemplate::parseExpression(ReadBuffer & istr, const FormatSettings & settings)
{
    size_t cur_column = 0;
    try
    {
        ParserKeyword parser_null("NULL");
        ParserNumber parser_number;
        size_t cur_token = 0;
        while (cur_column < literals.columns())
        {
            size_t skip_tokens_until = token_after_literal_idx[cur_column];
            while (cur_token < skip_tokens_until)
            {
                // TODO skip comments
                skipWhitespaceIfAny(istr);
                assertString(tokens[cur_token++], istr);
            }
            skipWhitespaceIfAny(istr);

            const IDataType & type = *literals.getByPosition(cur_column).type;
            if (need_special_parser[cur_column])
            {
                WhichDataType type_info(type);
                bool nullable = type_info.isNullable();
                if (nullable)
                    type_info = WhichDataType(dynamic_cast<const DataTypeNullable &>(type).getNestedType());

                Tokens tokens_number(istr.position(), istr.buffer().end());
                IParser::Pos iterator(tokens_number);
                Expected expected;
                ASTPtr ast;
                if (nullable && parser_null.parse(iterator, ast, expected))
                    ast = std::make_shared<ASTLiteral>(Field());
                else if (!parser_number.parse(iterator, ast, expected))
                    throw DB::Exception("Cannot parse literal", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
                istr.position() = const_cast<char *>(iterator->begin);
                Field & number = ast->as<ASTLiteral&>().value;

                // TODO also check type of Array(T), if T is arithmetic
                if ((number.getType() == Field::Types::UInt64  && type_info.isUInt64())
                 || (number.getType() == Field::Types::Int64   && type_info.isInt64())
                 || (number.getType() == Field::Types::Float64 && type_info.isFloat64())
                 || nullable)
                {
                    columns[cur_column]->insert(number);
                }
                else
                    throw DB::Exception("Cannot parse literal", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
            }
            else
            {
                type.deserializeAsTextQuoted(*columns[cur_column], istr, settings);
            }

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

ColumnPtr ConstantExpressionTemplate::evaluateAll()
{
    Block evaluated = literals.cloneWithColumns(std::move(columns));
    columns = literals.cloneEmptyColumns();
    if (!literals.columns())
        evaluated.insert({ColumnConst::create(ColumnUInt8::create(1, 0), rows_count), std::make_shared<DataTypeUInt8>(), "_dummy"});
    actions_on_literals->execute(evaluated);

    if (!evaluated || evaluated.rows() == 0)
        throw Exception("Logical error: empty block after evaluation of batch of constant expressions",
                        ErrorCodes::LOGICAL_ERROR);

    if (!evaluated.has(result_column_name))
        throw Exception("Cannot evaluate template " + result_column_name + ", block structure:\n" + evaluated.dumpStructure(),
                        ErrorCodes::CANNOT_EVALUATE_EXPRESSION_TEMPLATE);

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
