#include <iostream>
#include <iomanip>

#include <Poco/String.h>

#include <dbms/src/IO/ReadBufferFromFileDescriptor.h>
#include <dbms/src/IO/ReadHelpers.h>

#include <dbms/src/Common/Stopwatch.h>

#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>


int main()
{
    try
    {
        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);

        using Strings = std::vector<std::string>;
        Strings user_agents;

        while (!in.eof())
        {
            user_agents.push_back(std::string());
            DB::readEscapedString(user_agents.back(), in);
            user_agents.back() = Poco::toLower(user_agents.back());
            DB::assertString("\n", in);
        }

        Stopwatch watch;
        UInt64 res = 0;

        UATraits uatraits("browser.xml", "profiles.xml", "extra.xml");
        UATraits::MatchedSubstrings matched_substrings;

        for (const auto & string : user_agents)
        {
            UATraits::Result result;
            uatraits.detect(string, StringRef(), StringRef(), result, matched_substrings);
            res += result.bool_fields[UATraits::Result::isMobile];
        }

        double seconds = watch.elapsedSeconds();
        std::cerr << std::fixed << std::setprecision(2)
            << "Res = " << res << ". Elapsed: " << seconds << " sec., " << user_agents.size() / seconds << " rows/sec."
            << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.displayText() << "\nStack trace:\n" << e.getStackTrace().toString() << "\n";
        return 1;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
