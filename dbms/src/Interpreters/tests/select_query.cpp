#include <iostream>
#include <iomanip>

#include <boost/assign/list_inserter.hpp>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/NumberParser.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>
#include <DB/Storages/StorageFactory.h>

#include <DB/Functions/FunctionsLibrary.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Interpreters/executeQuery.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
		Yandex::DateLUTSingleton::instance();
		
		DB::Context context;

		context.functions = DB::FunctionsLibrary::get();
		context.path = "./";
		
		context.aggregate_function_factory		= new DB::AggregateFunctionFactory;
		context.data_type_factory				= new DB::DataTypeFactory;
		context.storage_factory					= new DB::StorageFactory;

		DB::loadMetadata(context);

		(*context.databases)["system"]["one"] 		= new DB::StorageSystemOne("one");
		(*context.databases)["system"]["numbers"] 	= new DB::StorageSystemNumbers("numbers");
		context.current_database = "default";

		DB::ReadBufferFromIStream in(std::cin);
		DB::WriteBufferFromOStream out(std::cout);
		DB::BlockInputStreamPtr query_plan;
		
		DB::executeQuery(in, out, context, query_plan);

		if (query_plan)
		{
/*			std::cerr << std::endl;
			query_plan->dumpTreeWithProfile(std::cerr);*/
			std::cerr << std::endl;
			query_plan->dumpTree(std::cerr);
			std::cerr << std::endl;
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString();
		return 1;
	}

	return 0;
}
