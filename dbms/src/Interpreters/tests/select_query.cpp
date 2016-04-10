#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/ConsoleChannel.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/Storages/System/StorageSystemOne.h>
#include <DB/Storages/StorageFactory.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Databases/IDatabase.h>
#include <DB/Databases/DatabaseOrdinary.h>


using namespace DB;

int main(int argc, char ** argv)
try
{
	Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
	Logger::root().setChannel(channel);
	Logger::root().setLevel("trace");

	/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
	DateLUT::instance();

	Context context;

	context.setPath("./");

	loadMetadata(context);

	DatabasePtr system = std::make_shared<DatabaseOrdinary>("system", "./metadata/system/", context, nullptr);

	context.addDatabase("system", system);
	system->attachTable("one", 	StorageSystemOne::create("one"));
	system->attachTable("numbers", StorageSystemNumbers::create("numbers"));
	context.setCurrentDatabase("default");

	ReadBufferFromIStream in(std::cin);
	WriteBufferFromOStream out(std::cout);
	BlockInputStreamPtr query_plan;

	executeQuery(in, out, context, query_plan, {});

	if (query_plan)
	{
/*			std::cerr << std::endl;
		query_plan->dumpTreeWithProfile(std::cerr);*/
		std::cerr << std::endl;
		query_plan->dumpTree(std::cerr);
		std::cerr << std::endl;
	}

	return 0;
}
catch (const Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl
		<< std::endl
		<< "Stack trace:" << std::endl
		<< e.getStackTrace().toString();
	return 1;
}
