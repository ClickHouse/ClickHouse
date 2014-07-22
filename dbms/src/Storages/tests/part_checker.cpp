#include <DB/Storages/MergeTree/MergeTreePartChecker.h>


int main(int argc, char ** argv)
{
	Logger::root().setChannel(new Poco::ConsoleChannel(std::cout));
	Logger::root().setLevel("trace");

	if ((argc != 3 && argc != 4) || (strcmp(argv[2], "0") && strcmp(argv[2], "1")))
	{
		std::cerr << "usage: " << argv[0] << " path strict [index_granularity]" << std::endl;
		return 1;
	}

	try
	{
		DB::MergeTreePartChecker::checkDataPart(argv[1], argc == 4 ? DB::parse<size_t>(argv[3]) : 8192ul, argv[2][0] == '1',
												DB::DataTypeFactory(), true);
	}
	catch (...)
	{
		DB::tryLogCurrentException(__PRETTY_FUNCTION__);
		throw;
	}

	return 0;
}
