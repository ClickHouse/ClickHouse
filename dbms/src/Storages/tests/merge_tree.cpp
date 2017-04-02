#include <iostream>

#include <Storages/StorageMergeTree.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/copyData.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>


int main(int argc, char ** argv)
try
{
    using namespace DB;

    const size_t rows = 12345;

    Context context;

    /// create a table with a pair of columns

    NamesAndTypesListPtr names_and_types = std::make_shared<NamesAndTypesList>();
    names_and_types->push_back(NameAndTypePair("d", std::make_shared<DataTypeDate>()));
    names_and_types->push_back(NameAndTypePair("a", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>())));

    ASTPtr primary_expr;
    Expected expected = "";
    String primary_expr_str = "d";
    const char * begin = primary_expr_str.data();
    const char * end = begin + primary_expr_str.size();
    const char * max_parsed_pos = begin;
    ParserExpressionList parser(false);
    if (!parser.parse(begin, end, primary_expr, max_parsed_pos, expected))
        throw Poco::Exception("Cannot parse " + primary_expr_str);

    MergeTreeData::MergingParams params;
    params.mode = MergeTreeData::MergingParams::Ordinary;

    StoragePtr table = StorageMergeTree::create(
        "./", "default", "test",
        names_and_types, {}, {}, ColumnDefaults{}, false,
        context, primary_expr, "d",
        nullptr, 101, params, false, {});

    /// write into it
    {
        Block block;

        ColumnWithTypeAndName column1;
        column1.name = "d";
        column1.type = table->getDataTypeByName("d");
        column1.column = column1.type->createColumn();
        ColumnUInt16::Container_t & vec1 = typeid_cast<ColumnUInt16 &>(*column1.column).getData();

        vec1.resize(rows);
        for (size_t i = 0; i < rows; ++i)
            vec1[i] = 10000;

        block.insert(column1);

        ColumnWithTypeAndName column2;
        column2.name = "a";
        column2.type = table->getDataTypeByName("a");
        column2.column = column2.type->createColumn();

        for (size_t i = 0; i < rows; ++i)
            column2.column->insert(Array((rand() % 10) == 0 ? (rand() % 10) : 0, i));

        block.insert(column2);

        BlockOutputStreamPtr out = table->write({}, {});
        out->write(block);
    }

    /// read from it
    {
        Names column_names;
        column_names.push_back("d");
        column_names.push_back("a");

        QueryProcessingStage::Enum stage;

        ASTPtr select;
        Expected expected = "";
        String select_str = "SELECT * FROM test";
        const char * begin = select_str.data();
        const char * end = begin + select_str.size();
        const char * max_parsed_pos = begin;
        ParserSelectQuery parser;
        if (!parser.parse(begin, end, select, max_parsed_pos, expected))
            throw Poco::Exception("Cannot parse " + primary_expr_str);

        BlockInputStreamPtr in = table->read(column_names, select, context, Settings(), stage)[0];

        Block sample;
        {
            ColumnWithTypeAndName col;
            col.type = names_and_types->front().type;
            sample.insert(std::move(col));
        }
        {
            ColumnWithTypeAndName col;
            col.type = names_and_types->back().type;
            sample.insert(std::move(col));
        }

        WriteBufferFromFileDescriptor out_buf(STDOUT_FILENO);

        RowOutputStreamPtr output_ = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample);
        BlockOutputStreamFromRowOutputStream output(output_);

        copyData(*in, output);
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.displayText() << ", stack trace: \n\n" << e.getStackTrace().toString() << std::endl;
    throw;
}
