#include <iostream>
#include <zkutil/Lock.h>

int main()
{

	try
	{
		zkutil::Lock l(std::make_shared<zkutil::ZooKeeper>("localhost:2181"), "/test", "test_lock");
		std::cout << "check " << l.check() << std::endl;
		std::cout << "lock tryLock() " << l.tryLock() << std::endl;
		std::cout << "check " << l.check() << std::endl;
	}
	catch (const Poco::Exception & e)
	{
		std::cout << e.message() << std::endl;
	}
	return 0;
}
