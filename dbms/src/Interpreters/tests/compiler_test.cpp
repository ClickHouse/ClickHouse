#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>

#include <DB/Interpreters/Compiler.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
	Logger::root().setChannel(channel);
	Logger::root().setLevel("trace");

	try
	{
		Compiler compiler(".", 1);

		auto lib = compiler.getOrCount("xxx", 1, "", []() -> std::string
		{
			return "void f() __attribute__((__visibility__(\"default\"))); void f() {}";
		}, [](SharedLibraryPtr&){});
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
	}

	return 0;
}
