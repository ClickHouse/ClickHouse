#pragma once

#include <iostream>
#include <Poco/TextEncoding.h>
#include <Poco/Util/ServerApplication.h>

#define YANDEX_APP_SERVER_MAIN_FUNC(AppServerClassName, main_func) \
            int \
            main_func (int _argc, char* _argv[]) \
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

#define YANDEX_APP_MAIN_FUNC(AppClassName, main_func) \
            int \
            main_func (int _argc, char* _argv[]) \
            { \
                AppClassName app; \
                try \
                { \
                    app.init(_argc, _argv); \
                    return app.run(); \
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

#define YANDEX_APP_SERVER_MAIN(AppServerClassName) YANDEX_APP_SERVER_MAIN_FUNC(AppServerClassName, main)

#define YANDEX_APP_MAIN(AppClassName) YANDEX_APP_MAIN_FUNC(AppClassName, main)
