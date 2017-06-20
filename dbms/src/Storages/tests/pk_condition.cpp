#include <iostream>
#include <Storages/MergeTree/PKCondition.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>


using namespace DB;

std::unique_ptr<PKCondition> cond;

void check(UInt64 left, UInt64 right, bool can_be_true)
{
    Field fleft = left;
    Field fright = right;
    if (cond->mayBeTrueInRange(1, &fleft, &fright, {}) != can_be_true)
    {
        std::cout << "failed range [" << left << ", " << right << "]" << std::endl;
        exit(2);
    }
}

int main(int argc, const char ** argv)
{
    std::string input = "SELECT count() FROM pre.t WHERE (key > 9000 AND key < 100000 OR key > 200000 AND key < 1000000 OR key > 3000000 AND key < 8000000 OR key > 12000000)";

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

    Context context = Context::createGlobal();
    NamesAndTypesList columns{{"key", std::make_shared<DataTypeUInt64>()}};
    Block sample_block{{DataTypeUInt64{}.createColumn(), std::make_shared<DataTypeUInt64>(), "key"}};
    SortDescription sort_descr;
    sort_descr.push_back(SortColumnDescription("key", 1, 1));

    cond = std::make_unique<PKCondition>(ast, context, columns, sort_descr, sample_block);
    std::cout << "condition: " << cond->toString() << std::endl;

    check(100, 1000, false);
    check(1000, 9000, false);
    check(1000, 9001, true);
    check(9000, 9001, true);
    check(9001, 9001, true);
    check(120000, 130000, false);

    std::cout << "passed" << std::endl;

    return 0;
}
