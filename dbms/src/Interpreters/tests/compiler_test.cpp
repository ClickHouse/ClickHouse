#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>

#include <Interpreters/Compiler.h>


int main(int, char **)
{
    using namespace DB;

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Logger::root().setChannel(channel);
    Logger::root().setLevel("trace");

    /// Check exception handling and catching
    try
    {
        Compiler compiler(".", 1);

        auto lib = compiler.getOrCount("catch_me_if_you_can", 0, "", []() -> std::string
        {
            return
                "#include <iostream>\n"
                "void f() __attribute__((__visibility__(\"default\")));\n"
                "void f()"
                "{"
                    "try { throw std::runtime_error(\"Catch me if you can\"); }"
                    "catch (const std::runtime_error & e) { std::cout << \"Caught in .so: \" << e.what() << std::endl; throw; }\n"
                "}"
                ;
        }, [](SharedLibraryPtr &){});

        auto f = lib->template get<void (*)()>("_Z1fv");

        try
        {
            f();
        }
        catch (const std::exception & e)
        {
            std::cout << "Caught in main(): " << e.what() << "\n";
            return 0;
        }
        catch (...)
        {
            std::cout << "Unknown exception\n";
            return -1;
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << "\n";
        return -1;
    }

    return 0;
}
