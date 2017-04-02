#include <iostream>
#include <iomanip>

#include <Common/Stopwatch.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/FunctionsArithmetic.h>


int main(int argc, char ** argv)
try
{
    using namespace DB;

    size_t n = atoi(argv[1]);

    ColumnWithTypeAndName descr1;
    auto col1 = std::make_shared<ColumnUInt8>();
    descr1.type = std::make_shared<DataTypeUInt8>();
    descr1.column = col1;
    descr1.name = "x";
    col1->getData().resize(n);

    ColumnWithTypeAndName descr2;
    auto col2 = std::make_shared<ColumnInt16>();
    descr2.type = std::make_shared<DataTypeInt16>();
    descr2.column = col2;
    descr2.name = "x";

    Block block;
    block.insert(descr1);
    block.insert(descr2);
    col2->getData().resize(n);

    for (size_t i = 0; i < n; ++i)
    {
        col1->getData()[i] = 10;
        col2->getData()[i] = 3;
    }

    FunctionDivideFloating f;
    DataTypes arg_types;
    arg_types.push_back(descr1.type);
    arg_types.push_back(descr2.type);

    ColumnNumbers arg_nums;
    arg_nums.push_back(0);
    arg_nums.push_back(1);

    size_t res_num = 2;

    DataTypePtr res_type = f.getReturnType(arg_types);

    ColumnWithTypeAndName descr_res;
    descr_res.type = res_type;
    descr_res.name = "z";

    {
        Stopwatch stopwatch;
        stopwatch.start();

        f.execute(block, arg_nums, res_num);

        stopwatch.stop();
        std::cout << std::fixed << std::setprecision(2)
            << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
            << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
            << std::endl;
    }

    Float64 x = 0;
    for (size_t i = 0; i < n; ++i)
        x += get<Float64>((*block.safeGetByPosition(2).column)[i]);

    std::cout << x << std::endl;
    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    throw;
}
