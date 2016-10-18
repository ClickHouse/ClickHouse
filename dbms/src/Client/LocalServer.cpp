#include <Poco/Util/Application.h>
#include <DB/Interpreters/Context.h>

namespace DB
{

class LocalServer : public Poco::Util::Application
{
	LocalServer() {}

	int main(const std::vector<std::string> & args);

protected:

	std::unique_ptr<Context> context_global;
};

}

int main(int argc, char ** argv)
{
	return 0;
}
