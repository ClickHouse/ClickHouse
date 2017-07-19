#pragma once

#include <iostream>
#include <Poco/TextEncoding.h>
#include <Poco/Util/ServerApplication.h>


class AppHandler
{
public:
    template <typename App>
    static void init(App & app, int _argc, char * _argv[])
    {
        app.init(_argc, _argv);
    }

    template <typename App>
    static int run(App & app, int _argc, char * _argv[])
    {
        return app.run();
    }
};

class ServerAppHandler
{
public:
    template <typename App>
    static void init(App & app, int _argc, char * _argv[])
    {
    }

    template <typename App>
    static int run(App & app, int _argc, char * _argv[])
    {
        return app.run(_argc, _argv);
    }
};

template <typename App, typename AppHandler>
class YandexAppMainFuncImpl
{
public:
    static int main(int _argc, char * _argv[])
    {
        App app;
        try
        {
            AppHandler::init(app, _argc, _argv);
            return AppHandler::run(app, _argc, _argv);
        }
        catch (const Poco::Exception & _ex)
        {
            std::cerr << "POCO ERROR: " << _ex.displayText() << std::endl;
            app.logger().log(_ex);
        }
        catch (const std::exception & _ex)
        {
            std::cerr << "STD ERROR: " << _ex.what() << std::endl;
            app.logger().error(Poco::Logger::format ("Got exception: $0", _ex.what()));
        }
        catch (...)
        {
            std::cerr << "UNKNOWN ERROR" << std::endl;
            app.logger().error("Unknown exception");
        }
        return Poco::Util::Application::EXIT_CONFIG;
    }
};


#define YANDEX_APP_SERVER_MAIN_FUNC(AppServerClassName, main_func) int main_func(int _argc, char * _argv[]) { return YandexAppMainFuncImpl<AppServerClassName, ServerAppHandler>::main(_argc, _argv); }

#define YANDEX_APP_MAIN_FUNC(AppClassName, main_func) int main_func(int _argc, char * _argv[]) { return YandexAppMainFuncImpl<AppClassName, AppHandler>::main(_argc, _argv); }

#define YANDEX_APP_SERVER_MAIN(AppServerClassName) YANDEX_APP_SERVER_MAIN_FUNC(AppServerClassName, main)

#define YANDEX_APP_MAIN(AppClassName) YANDEX_APP_MAIN_FUNC(AppClassName, main)
