#include <iostream>
#include <iomanip>

#include <Poco/String.h>

#include <dbms/src/IO/ReadBufferFromFileDescriptor.h>
#include <dbms/src/IO/ReadHelpers.h>

#include <dbms/src/Common/Stopwatch.h>

#include <metrika/core/libs/uatraits-fast/UATraitsCaseSafeCached.h>
#include <uatraits/detector.hpp>


struct StringSet
{
    std::string original;
    std::string lowercase;
};

using Strings = std::vector<StringSet>;

int main(int argc, char ** argv)
{
    try
    {
        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);

        UATraitsCaseSafeCached uatraits_fast("browser.xml", "profiles.xml");
        uatraits::detector uatraits_slow("browser.xml", "profiles.xml");

        typedef std::vector<std::string> Strings;
        Strings user_agents;

        while (!in.eof())
        {
            user_agents.push_back(StringSet());
            DB::readEscapedString(user_agents.back().original, in);
            user_agents.back().lowercase = Poco::toLower(user_agents.back().original);
            DB::assertString("\n", in);
        }

        Stopwatch watch;

        UATraits::MatchedSubstrings matched_substrings;
        for (Strings::const_iterator it = user_agents.begin(); it != user_agents.end(); ++it)
        {
            UATraits::Result result_fast;
            uatraits::detector::result_type result_slow;

            uatraits_fast.detect(it->original, it->lowercase, StringRef(), StringRef(), StringRef(), result_fast, matched_substrings);
            uatraits_slow.detect(it->lowercase, result_slow);

            if ((result_slow["isMobile"] == "true")
                != result_fast.bool_fields[UATraits::Result::isMobile])
                throw Poco::Exception("Difference in isMobile at row " + *it);

            if (result_slow["BrowserName"]
                != result_fast.string_ref_fields[UATraits::Result::BrowserName])
                throw Poco::Exception("Difference in BrowserName at row " + *it);

            if (result_slow["BrowserVersion"]
                != result_fast.version_fields[UATraits::Result::BrowserVersion].toString())
                throw Poco::Exception("Difference in BrowserVersion at row " + *it);
            
            if (result_slow["BrowserBase"]
                != result_fast.string_ref_fields[UATraits::Result::BrowserBase].toString())
                throw Poco::Exception("Difference in BrowserBase at row " + *it);
        }

        double seconds = watch.elapsedSeconds();
        std::cerr << std::fixed << std::setprecision(2)
            << "Ok. Elapsed: " << seconds << " sec., " << user_agents.size() / seconds << " rows/sec."
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
