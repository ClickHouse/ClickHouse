#pragma once

#include <iostream>
#include <Poco/TextEncoding.h>
#include <Poco/Util/ServerApplication.h>

#define YANDEX_APP_SERVER_MAIN(AppServerClassName) \
			int \
			main (int _argc, char* _argv[]) \
			{ \
				AppServerClassName app; \
				try \
				{ \
					return app.run (_argc, _argv); \
				} \
				catch (const Poco::Exception& _ex) \
				{ \
					std::cerr << "POCO ERROR: " << _ex.displayText() << std::endl; \
					app.logger().log (_ex); \
				} \
				catch (const std::exception& _ex) \
				{ \
					std::cerr << "STD ERROR: " << _ex.what() << std::endl; \
					app.logger().error (Poco::Logger::format ("Got exception: $0", _ex.what ())); \
				} \
				catch (...) \
				{ \
					std::cerr << "UNKNOWN ERROR" << std::endl; \
					app.logger().error ("Unknown exception");  \
				} \
				return Poco::Util::Application::EXIT_CONFIG; \
			}
