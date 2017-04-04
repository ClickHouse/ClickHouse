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

#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/copyData.h>

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
        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

        formatAST(*ast, std::cerr);
        std::cerr << std::endl;

        Context context;
        NamesAndTypesList columns
        {
            {"x", std::make_shared<DataTypeInt16>()},
            {"s1", std::make_shared<DataTypeString>()},
            {"s2", std::make_shared<DataTypeString>()}
        };

        ExpressionAnalyzer analyzer(ast, context, {}, columns);
        ExpressionActionsChain chain;
        analyzer.appendSelect(chain, false);
        analyzer.appendProjectResult(chain, false);
        chain.finalize();
        ExpressionActionsPtr expression = chain.getLastActions();

        size_t n = argc == 2 ? atoi(argv[1]) : 10;

        Block block;

        ColumnWithTypeAndName column_x;
        column_x.name = "x";
        column_x.type = std::make_shared<DataTypeInt16>();
        auto x = std::make_shared<ColumnInt16>();
        column_x.column = x;
        auto & vec_x = x->getData();

        vec_x.resize(n);
        for (size_t i = 0; i < n; ++i)
            vec_x[i] = i;

        block.insert(column_x);

        const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

        ColumnWithTypeAndName column_s1;
        column_s1.name = "s1";
        column_s1.type = std::make_shared<DataTypeString>();
        column_s1.column = std::make_shared<ColumnString>();

        for (size_t i = 0; i < n; ++i)
            column_s1.column->insert(String(strings[i % 5]));

        block.insert(column_s1);

        ColumnWithTypeAndName column_s2;
        column_s2.name = "s2";
        column_s2.type = std::make_shared<DataTypeString>();
        column_s2.column = std::make_shared<ColumnString>();

        for (size_t i = 0; i < n; ++i)
            column_s2.column->insert(String(strings[i % 3]));

        block.insert(column_s2);

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
        RowOutputStreamPtr os_ = std::make_shared<TabSeparatedRowOutputStream>(out_buf, block);
        BlockOutputStreamFromRowOutputStream os(os_);

        copyData(lis, os);
    }
    catch (const Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
