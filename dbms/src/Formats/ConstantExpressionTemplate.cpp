
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

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_EXPRESSION_TEMPLATE;
    extern const int CANNOT_PARSE_EXPRESSION_USING_TEMPLATE;
    extern const int CANNOT_EVALUATE_EXPRESSION_TEMPLATE;
}


ConstantExpressionTemplate::ConstantExpressionTemplate(const IDataType & result_column_type, TokenIterator begin, TokenIterator end,
                                                       const Context & context)
{
    std::pair<String, NamesAndTypesList> expr_template = replaceLiteralsWithDummyIdentifiers(begin, end);
    for (const auto & col : expr_template.second)
        literals.insert({nullptr, col.type, col.name});
    columns = literals.cloneEmptyColumns();

    ParserExpression parser;
    Expected expected;
    Tokens template_tokens(expr_template.first.data(), expr_template.first.data() + expr_template.first.size());
    TokenIterator token_iterator1(template_tokens);

    ASTPtr ast_template;
    if (!parser.parse(token_iterator1, ast_template, expected))
        throw Exception("Cannot parse template after replacing literals: ", ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);

    addNodesToCastResult(result_column_type, ast_template);
    result_column_name = ast_template->getColumnName();

    auto syntax_result = SyntaxAnalyzer(context).analyze(ast_template, expr_template.second);

    actions_on_literals = ExpressionAnalyzer(ast_template, syntax_result, context).getActions(false);
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
                // TODO skip comments
                skipWhitespaceIfAny(istr);
                assertString(tokens[cur_token++], istr);
            }
            skipWhitespaceIfAny(istr);
            const IDataType & type = *literals.getByPosition(cur_column).type;
            type.deserializeAsTextQuoted(*columns[cur_column], istr, settings);
            ++cur_column;
        }
        while (cur_token < tokens.size())
        {
            skipWhitespaceIfAny(istr);
            assertString(tokens[cur_token++], istr);
        }
    } catch (DB::Exception & e)
    {
        for (size_t i = 0; i < cur_column; ++i)
            columns[i]->popBack(1);

        if (!isParseError(e.code()))
            throw;
    }
    throw DB::Exception("Cannot parse expression using template", ErrorCodes::CANNOT_PARSE_EXPRESSION_USING_TEMPLATE);
}

ColumnPtr ConstantExpressionTemplate::evaluateAll()
{
    Block evaluated = literals.cloneWithColumns(std::move(columns));
    columns = literals.cloneEmptyColumns();
    actions_on_literals->execute(evaluated);

    if (!evaluated || evaluated.rows() == 0)
        throw Exception("Logical error: empty block after evaluation of batch of constant expressions",
                        ErrorCodes::LOGICAL_ERROR);

    if (!evaluated.has(result_column_name))
        throw Exception("Cannot evaluate template " + result_column_name + ", block structure:\n" + evaluated.dumpStructure(),
                        ErrorCodes::CANNOT_EVALUATE_EXPRESSION_TEMPLATE);

    return evaluated.getByName(result_column_name).column;
}

std::pair<String, NamesAndTypesList>
ConstantExpressionTemplate::replaceLiteralsWithDummyIdentifiers(TokenIterator & begin, TokenIterator & end)
{
    NamesAndTypesList dummy_columns;
    ParserLiteral parser;
    String result;
    size_t token_idx = 0;
    while (begin != end)
    {
        const Token & t = *begin;
        if (t.isError())
            throw DB::Exception("Error in tokens", ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);

        // TODO don't convert constant string arguments of functions such as CAST(x, 'type')
        // TODO process Array as one literal to make possible parsing constant arrays of different size
        if (t.type == TokenType::Number || t.type == TokenType::StringLiteral)
        {
            Expected expected;
            ASTPtr ast;
            if (!parser.parse(begin, ast, expected))
                throw DB::Exception("Cannot determine literal type", ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);

            // TODO use nullable type if necessary (e.g. value is not NULL, but result_column_type is nullable and next rows may contain NULLs)
            // TODO parse numbers more carefully: sign is a separate token before number
            Field & value = ast->as<ASTLiteral &>().value;
            DataTypePtr type = DataTypeFactory::instance().get(value.getTypeName());
            // TODO ensure dummy_col_name is unique (there was no _dummy_x identifier in expression)
            String dummy_col_name = "_dummy_" + std::to_string(dummy_columns.size());
            dummy_columns.push_back(NameAndTypePair(dummy_col_name, type));
            token_after_literal_idx.push_back(token_idx);
            result.append(dummy_col_name);
        }
        else
        {
            tokens.emplace_back(t.begin, t.size());
            result.append(tokens.back());
            ++begin;
            ++token_idx;
        }
        result.append(" ");
    }
    if (dummy_columns.empty())  // TODO
        throw DB::Exception("not implemented yet", ErrorCodes::CANNOT_CREATE_EXPRESSION_TEMPLATE);
    return std::make_pair(result, dummy_columns);
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
