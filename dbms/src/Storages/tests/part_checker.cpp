#include <Poco/ConsoleChannel.h>
#include <DB/Storages/MergeTree/MergeTreePartChecker.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
	Logger::root().setChannel(channel);
	Logger::root().setLevel("trace");

	if ((argc != 3 && argc != 4) || (strcmp(argv[2], "0") && strcmp(argv[2], "1")))
	{
		std::cerr << "usage: " << argv[0] << " path strict [index_granularity]" << std::endl;
		return 1;
	}

	try
	{
		MergeTreePartChecker::Settings settings;
		if (argc == 4)
			settings.setIndexGranularity(parse<size_t>(argv[3]));
		settings.setRequireChecksums(argv[2][0] == '1');
		settings.setRequireColumnFiles(argv[2][0] == '1');
		settings.setVerbose(true);

		MergeTreePartChecker::checkDataPart(argv[1], settings, {});
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
		throw;
	}

	return 0;
}
