
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Formats/ConstantExpressionTemplate.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_EXPRESSION_TEMPLATE;
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
    extern const int CANNOT_EVALUATE_EXPRESSION_TEMPLATE;
}

class ReplaceLiteralsMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ReplaceLiteralsMatcher, true>;

    struct LiteralInfo
    {
        typedef std::shared_ptr<ASTLiteral> ASTLiteralPtr;
        LiteralInfo(const ASTLiteralPtr & literal_, const String & column_name_) : literal(literal_), dummy_column_name(column_name_) { }
        ASTLiteralPtr literal;
        String dummy_column_name;
    };

    using Data = std::vector<LiteralInfo>;

    static void visit(ASTPtr & ast, Data & data)
    {
        auto literal = std::dynamic_pointer_cast<ASTLiteral>(ast);
        if (!literal || !literal->begin || !literal->end)
            return;

        // TODO don't replace constant arguments of functions such as CAST(x, 'type')
        // TODO ensure column_name is unique (there was no _dummy_x identifier in expression)
        String column_name = "_dummy_" + std::to_string(data.size());
        data.emplace_back(literal, column_name);
        ast = std::make_shared<ASTIdentifier>(column_name);
    }
    static bool needChildVisit(ASTPtr & node, const ASTPtr &) { return !node->as<ASTLiteral>(); }
};

using ReplaceLiteralsVisitor = ReplaceLiteralsMatcher::Visitor;
using LiteralInfo = ReplaceLiteralsMatcher::LiteralInfo;


ConstantExpressionTemplate::ConstantExpressionTemplate(const IDataType & result_column_type,
                                                       TokenIterator expression_begin, TokenIterator expression_end,
                                                       const ASTPtr & expression_, const Context & context)
{
    ASTPtr expression = expression_->clone();
    addNodesToCastResult(result_column_type, expression);
    ReplaceLiteralsVisitor::Data replaced_literals;
    ReplaceLiteralsVisitor(replaced_literals).visit(expression);

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
        else
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
        ParserNumber parser;
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
                Tokens tokens_number(istr.position(), istr.buffer().end());
                TokenIterator iterator(tokens_number);
                Expected expected;
                ASTPtr ast;
                if (!parser.parse(iterator, ast, expected))
                    throw DB::Exception("Cannot parse literal", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
                istr.position() = const_cast<char *>(iterator->begin);
                Field number = ast->as<ASTLiteral&>().value;

                WhichDataType type_info(type);
                if ((number.getType() == Field::Types::UInt64  && type_info.isUInt64())
                 || (number.getType() == Field::Types::Int64   && type_info.isInt64())
                 || (number.getType() == Field::Types::Float64 && type_info.isFloat64()))
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
