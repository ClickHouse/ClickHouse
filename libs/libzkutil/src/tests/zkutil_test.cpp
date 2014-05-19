#include <zkutil/ZooKeeper.h>
#include <zkutil/KeeperException.h>
#include <iostream>
#include <readline/readline.h>
#include <readline/history.h>
#include <sstream>
#include <Poco/ConsoleChannel.h>
#include <Yandex/logger_useful.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ReadBufferFromString.h>


void printStat(const zkutil::Stat & s)
{
	std::cout << "Stat:\n";
	std::cout << "  czxid: " << s.getczxid() << '\n';
	std::cout << "  mzxid: " << s.getmzxid() << '\n';
	std::cout << "  ctime: " << s.getctime() << '\n';
	std::cout << "  mtime: " << s.getmtime() << '\n';
	std::cout << "  version: " << s.getversion() << '\n';
	std::cout << "  cversion: " << s.getcversion() << '\n';
	std::cout << "  aversion: " << s.getaversion() << '\n';
	std::cout << "  ephemeralOwner: " << s.getephemeralOwner() << '\n';
	std::cout << "  dataLength: " << s.getdataLength() << '\n';
	std::cout << "  numChildren: " << s.getnumChildren() << '\n';
	std::cout << "  pzxid: " << s.getpzxid() << std::endl;
}

void waitForWatch(zkutil::WatchFuture & future)
{
	std::cout << "waiting for watch" << std::endl;
	zkutil::WatchEventInfo res = future.get();
	std::cout << "event: " << zkutil::WatchEvent::toString(res.event) << std::endl;
}


void readUntilSpace(std::string & s, DB::ReadBuffer & buf)
{
	s = "";
	while (!buf.eof())
	{
		if (isspace(*buf.position()))
			return;
		s.push_back(*buf.position());
		++buf.position();
	}
}

void readMaybeQuoted(std::string & s, DB::ReadBuffer & buf)
{
	if (!buf.eof() && *buf.position() == '\'')
		DB::readQuotedString(s, buf);
	else
		readUntilSpace(s, buf);
}


int main(int argc, char ** argv)
{
	try
	{
		if (argc != 2)
		{
			std::cerr << "usage: " << argv[0] << " hosts" << std::endl;
			return 2;
		}

		Logger::root().setChannel(new Poco::ConsoleChannel(std::cerr));
		Logger::root().setLevel("trace");

		zkutil::ZooKeeper zk(argv[1]);

		while (char * line_ = readline(":3 "))
		{
			add_history(line_);
			std::string line(line_);
			free(line_);

			try
			{
				std::stringstream ss(line);

				std::string cmd;
				ss >> cmd;

				if (cmd == "q" || cmd == "quit" || cmd == "exit" || cmd == ":q")
					break;

				std::string path;
				ss >> path;
				if (cmd == "ls")
				{
					std::string w;
					ss >> w;
					bool watch = w == "w";
					zkutil::WatchFuture future;
					std::vector<std::string> v = zk.getChildren(path, nullptr, watch ? &future : nullptr);
					for (size_t i = 0; i < v.size(); ++i)
					{
						std::cout << v[i] << std::endl;
					}
					if (watch)
						waitForWatch(future);
				}
				else if (cmd == "create")
				{
					DB::ReadBufferFromString in(line);

					std::string path;
					std::string data;
					std::string mode;

					DB::assertString("create", in);
					DB::skipWhitespaceIfAny(in);
					readMaybeQuoted(path, in);
					DB::skipWhitespaceIfAny(in);
					readMaybeQuoted(data, in);
					DB::skipWhitespaceIfAny(in);
					readUntilSpace(mode, in);

					zkutil::CreateMode::type m;
					if (mode == "p")
						m = zkutil::CreateMode::Persistent;
					else if (mode == "ps")
						m = zkutil::CreateMode::PersistentSequential;
					else if (mode == "e")
						m = zkutil::CreateMode::Ephemeral;
					else if (mode == "es")
						m = zkutil::CreateMode::EphemeralSequential;
					else
					{
						std::cout << "Bad create mode" << std::endl;
						continue;
					}
					std::cout << zk.create(path, data, m) << std::endl;
				}
				else if (cmd == "remove")
				{
					zk.remove(path);
				}
				else if (cmd == "rmr")
				{
					zk.removeRecursive(path);
				}
				else if (cmd == "exists")
				{
					std::string w;
					ss >> w;
					bool watch = w == "w";
					zkutil::WatchFuture future;
					zkutil::Stat stat;
					bool e = zk.exists(path, &stat, watch ? &future : nullptr);
					if (e)
						printStat(stat);
					else
						std::cout << path << " does not exist" << std::endl;
					if (watch)
						waitForWatch(future);
				}
				else if (cmd == "get")
				{
					std::string w;
					ss >> w;
					bool watch = w == "w";
					zkutil::WatchFuture future;
					zkutil::Stat stat;
					std::string data = zk.get(path, &stat, watch ? &future : nullptr);
					std::cout << "Data: " << data << std::endl;
					printStat(stat);
					if (watch)
						waitForWatch(future);
				}
				else if (cmd == "set")
				{
					DB::ReadBufferFromString in(line);

					std::string data;
					int version = -1;

					DB::assertString("set", in);
					DB::skipWhitespaceIfAny(in);
					readMaybeQuoted(data, in);
					DB::skipWhitespaceIfAny(in);

					if (!in.eof())
						DB::readText(version, in);

					zkutil::Stat stat;
					zk.set(path, data, version, &stat);
					printStat(stat);
				}
				else if (cmd != "")
				{
					std::cout << "commands:\n";
					std::cout << "  q\n";
					std::cout << "  ls path [w]\n";
					std::cout << "  create path data (p|ps|e|es)\n";
					std::cout << "  remove path\n";
					std::cout << "  rmr path\n";
					std::cout << "  exists path [w]\n";
					std::cout << "  get path [w]\n";
					std::cout << "  set path data [version]" << std::endl;
					continue;
				}

			}
			catch (zkutil::KeeperException & e)
			{
				std::cerr << "KeeperException: " << e.displayText() << std::endl;
			}
		}

		zk.close();
	}
	catch (zkutil::KeeperException & e)
	{
		std::cerr << "KeeperException: " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
