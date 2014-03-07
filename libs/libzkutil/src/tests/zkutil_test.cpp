#include <zkutil/ZooKeeper.h>
#include <zkutil/KeeperException.h>
#include <iostream>
#include <readline/readline.h>
#include <sstream>


int main(int argc, char ** argv)
{
	try
	{
		if (argc != 2)
		{
			std::cerr << "usage: " << argv[0] << " hosts" << std::endl;
			return 2;
		}

		zkutil::ZooKeeper zk(argv[1]);

		while (char * line = readline(":3 "))
		{
			try
			{
				std::stringstream ss(line);

				std::string cmd;
				if (!(ss >> cmd))
					continue;

				if (cmd == "q" || cmd == "quit" || cmd == "exit" || cmd == ":q")
					break;

				if (cmd == "help")
				{
					std::cout << "commands: q, ls (not yet: stat, get, set, create, remove)" << std::endl;
					continue;
				}
				std::string path;
				ss >> path;
				if (cmd == "ls")
				{
					std::vector<std::string> v = zk.getChildren(path);
					for (size_t i = 0; i < v.size(); ++i)
					{
						std::cout << v[i] << std::endl;
					}
				}
			}
			catch (zkutil::KeeperException & e)
			{
				std::cerr << "KeeperException: " << e.displayText() << std::endl;
			}
		}
	}
	catch (zkutil::KeeperException & e)
	{
		std::cerr << "KeeperException: " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
