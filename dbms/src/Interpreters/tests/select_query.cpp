#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/ConsoleChannel.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>
#include <DB/Storages/StorageFactory.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Interpreters/executeQuery.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
		Logger::root().setChannel(channel);
		Logger::root().setLevel("trace");

		/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
		DateLUT::instance();

		DB::Context context;

		context.setPath("./");

		DB::loadMetadata(context);

		context.addDatabase("system");
		context.addTable("system", "one", 		DB::StorageSystemOne::create("one"));
		context.addTable("system", "numbers", 	DB::StorageSystemNumbers::create("numbers"));
		context.setCurrentDatabase("default");

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
		std::cerr << e.what() << ", " << e.displayText() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString();
		return 1;
	}

	return 0;
}
