#include <iostream>

#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <DataTypes/FunctionSignature.h>
#include <Interpreters/Context.h>

#include <Poco/Util/Application.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYNTAX_ERROR;
    }
}


using namespace DB;


class Test : public Poco::Util::Application
{
private:
    void initialize(Poco::Util::Application & self) override
    {
        Poco::Util::Application::initialize(self);
    }

    int main(const std::vector<std::string> & /*args*/) override
    try
    {
        const auto & factory = FunctionFactory::instance();
        auto names = factory.getAllRegisteredNames();

        Context context = Context::createGlobal();
        context.setGlobalContext(context);

        for (const auto & name : names)
        {
            const auto & function = factory.get(name, context);
            std::string signature = function->getSignature();

            if (signature.empty())
                continue;

            std::cerr << name << ": " << signature << "\n";

            FunctionSignature checker(signature);
        }

        return 0;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << std::endl;
        throw;
    }
};


int main(int argc, char ** argv)
try
{
    registerFunctions();
    Test app;
    app.init(argc, argv);
    return app.run();
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
