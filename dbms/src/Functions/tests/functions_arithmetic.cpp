#include <iostream>
#include <iomanip>

#include <Poco/Stopwatch.h>

#include <DB/Functions/FunctionsArithmetic.h>


int main(int argc, char ** argv)
{
	try
	{
		size_t n = atoi(argv[1]);

		DB::ColumnWithTypeAndName descr1;
		DB::ColumnUInt8 * col1 = new DB::ColumnUInt8;
		descr1.type = new DB::DataTypeUInt8;
		descr1.column = col1;
		descr1.name = "x";
		col1->getData().resize(n);

		DB::ColumnWithTypeAndName descr2;
		DB::ColumnInt16 * col2 = new DB::ColumnInt16;
		descr2.type = new DB::DataTypeInt16;
		descr2.column = col2;
		descr2.name = "x";

		DB::Block block;
		block.insert(descr1);
		block.insert(descr2);
		col2->getData().resize(n);

		for (size_t i = 0; i < n; ++i)
		{
			col1->getData()[i] = 10;
			col2->getData()[i] = 3;
		}

		DB::FunctionDivideFloating f;
		DB::DataTypes arg_types;
		arg_types.push_back(descr1.type);
		arg_types.push_back(descr2.type);

		DB::ColumnNumbers arg_nums;
		arg_nums.push_back(0);
		arg_nums.push_back(1);

		size_t res_num = 2;

		DB::DataTypePtr res_type = f.getReturnType(arg_types);

		DB::ColumnWithTypeAndName descr_res;
		descr_res.type = res_type;
		descr_res.name = "z";

		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();
			
			f.execute(block, arg_nums, res_num);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}

		DB::Float64 x = 0;
		for (size_t i = 0; i < n; ++i)
			x += DB::get<DB::Float64>((*block.getByPosition(2).column)[i]);

		std::cout << x << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
		throw;
	}
	
	return 0;
}
