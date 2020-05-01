#include <iostream>
#include <iomanip>

#include <IO/WriteBufferFromOStream.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <Formats/FormatFactory.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        std::string input = "SELECT x, s1, s2, "
            "/*"
            "2 + x * 2, x * 2, x % 3 == 1, "
            "s1 == 'abc', s1 == s2, s1 != 'abc', s1 != s2, "
            "s1 <  'abc', s1 <  s2, s1 >  'abc', s1 >  s2, "
            "s1 <= 'abc', s1 <= s2, s1 >= 'abc', s1 >= s2, "
            "*/"
            "s1 < s2 AND x % 3 < x % 5";

        ParserSelectQuery parser;
        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);

        formatAST(*ast, std::cerr);
        std::cerr << std::endl;

        SharedContextHolder shared_context = Context::createShared();
        Context context = Context::createGlobal(shared_context.get());
        context.makeGlobalContext();
        NamesAndTypesList columns
        {
            {"x", std::make_shared<DataTypeInt16>()},
            {"s1", std::make_shared<DataTypeString>()},
            {"s2", std::make_shared<DataTypeString>()}
        };

        auto syntax_result = SyntaxAnalyzer(context).analyze(ast, columns);
        SelectQueryExpressionAnalyzer analyzer(ast, syntax_result, context);
        ExpressionActionsChain chain(context);
        analyzer.appendSelect(chain, false);
        analyzer.appendProjectResult(chain);
        chain.finalize();
        ExpressionActionsPtr expression = chain.getLastActions();

        size_t n = argc == 2 ? std::stol(argv[1]) : 10;

        Block block;

        {
            ColumnWithTypeAndName column;
            column.name = "x";
            column.type = std::make_shared<DataTypeInt16>();
            auto col = ColumnInt16::create();
            auto & vec_x = col->getData();

            vec_x.resize(n);
            for (size_t i = 0; i < n; ++i)
                vec_x[i] = i % 9;

            column.column = std::move(col);
            block.insert(column);
        }

        const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

        {
            ColumnWithTypeAndName column;
            column.name = "s1";
            column.type = std::make_shared<DataTypeString>();
            auto col = ColumnString::create();

            for (size_t i = 0; i < n; ++i)
                col->insert(std::string(strings[i % 5]));

            column.column = std::move(col);
            block.insert(column);
        }

        {
            ColumnWithTypeAndName column;
            column.name = "s2";
            column.type = std::make_shared<DataTypeString>();
            auto col = ColumnString::create();

            for (size_t i = 0; i < n; ++i)
                col->insert(std::string(strings[i % 3]));

            column.column = std::move(col);
            block.insert(column);
        }

        {
            Stopwatch stopwatch;
            stopwatch.start();

            expression->execute(block);

            stopwatch.stop();
            std::cout << std::fixed << std::setprecision(2)
                << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
                << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
                << std::endl;
        }

        auto is = std::make_shared<OneBlockInputStream>(block);
        LimitBlockInputStream lis(is, 20, std::max(0, static_cast<int>(n) - 20));
        WriteBufferFromOStream out_buf(std::cout);
        BlockOutputStreamPtr out = FormatFactory::instance().getOutput("TabSeparated", out_buf, block, context);

        copyData(lis, *out);
    }
    catch (const Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
