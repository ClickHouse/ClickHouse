#include <DB/Storages/MergeTree/MergeTreePartChecker.h>


int main(int argc, char ** argv)
{
	Logger::root().setChannel(new Poco::ConsoleChannel(std::cout));
	Logger::root().setLevel("trace");

	if (argc != 2 && argc != 3)
	{
		std::cerr << "usage: " << argv[0] << " path [index_granularity]" << std::endl;
		return 1;
	}

	try
	{
		DB::MergeTreePartChecker::checkDataPart(argv[1], argc == 3 ? DB::parse<size_t>(argv[2]) : 8192ul, DB::DataTypeFactory());
	}
	catch (...)
	{
		DB::tryLogCurrentException(__PRETTY_FUNCTION__);
		throw;
	}

	return 0;
}
