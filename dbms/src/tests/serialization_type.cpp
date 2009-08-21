#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>

#include <Poco/Types.h>
#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/BinaryWriter.h>


class ISerialization
{
public:
	typedef std::vector<Poco::UInt64> Bulk;

	virtual void serialize(Poco::UInt64 x, std::ostream & ostr) = 0;
	virtual void bulkSerialize(const Bulk & bulk, std::ostream & ostr) = 0;

	virtual ~ISerialization() {}
};

class Serialization1 : public ISerialization
{
public:
	void serialize(Poco::UInt64 x, std::ostream & ostr)
	{
		ostr.write(reinterpret_cast<char*>(&x), sizeof(x));
	}
	
	void bulkSerialize(const Bulk & bulk, std::ostream & ostr)
	{
		ostr.write(reinterpret_cast<const char*>(&bulk[0]), bulk.size() * sizeof(bulk[0]));
	}
};

class Serialization2 : public ISerialization
{
public:
	void serialize(Poco::UInt64 x, std::ostream & ostr)
	{
		ostr.write(reinterpret_cast<char*>(&x), sizeof(x));
	}
	
	void bulkSerialize(const Bulk & bulk, std::ostream & ostr)
	{
		ostr.write(reinterpret_cast<const char*>(&bulk[0]), bulk.size() * sizeof(bulk[0]));
	}
};


int main(int argc, char ** argv)
{
	Poco::SharedPtr<ISerialization> serialization_virtual;
	if (time(0) % 2)
		serialization_virtual = new Serialization1;
	else
		serialization_virtual = new Serialization2;

	Serialization1 serialization_non_virtual;
		
	int n = 100000000 / 8;
	Poco::Stopwatch stopwatch;

	std::vector<Poco::UInt64> bulk(n, 0);

	{
		std::ofstream ostr("test1");
	
		stopwatch.restart();
		for (int i = 0; i < n; ++i)
			serialization_virtual->serialize(i, ostr);
		stopwatch.stop();
		std::cout << "Virtual: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ofstream ostr("test2");
	
		stopwatch.restart();
		for (int i = 0; i < n; ++i)
			serialization_non_virtual.serialize(i, ostr);
		stopwatch.stop();
		std::cout << "Non virtual: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ofstream ostr("test3");
	
		stopwatch.restart();
		serialization_virtual->bulkSerialize(bulk, ostr);
		stopwatch.stop();
		std::cout << "Virtual bulk: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	{
		std::ofstream ostr("test4");
	
		stopwatch.restart();
		serialization_non_virtual.bulkSerialize(bulk, ostr);
		stopwatch.stop();
		std::cout << "Non virtual bulk: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	return 0;
}
