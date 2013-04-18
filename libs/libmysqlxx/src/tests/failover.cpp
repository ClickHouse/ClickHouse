#include <mysqlxx/PoolWithFailover.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <iostream>

class App : public Poco::Util::Application
{
public:
	App() {}
};

int main()
{
	App app;
	app.loadConfiguration("failover.xml");
	
	Logger::root().setChannel(new Poco::ConsoleChannel(std::cout));
	Logger::root().setLevel("trace");
	
	mysqlxx::PoolWithFailover pool("mysql_goals");
	mysqlxx::PoolWithFailover::Entry conn = pool.Get();
	mysqlxx::Query Q = conn->query();
	Q << "SELECT count(*) FROM counters";
	mysqlxx::UseQueryResult R = Q.use();
	std::cout << R.fetch_row()[0] << std::endl;
	
	return 0;
}
