#include <iostream>

#include <Poco/String.h>

#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>


int main(int argc, char ** argv)
{
    try
    {
        UATraits uatraits("browser.xml", "profiles.xml", "extra.xml");

        StringRef user_agent;
        StringRef profile;
        StringRef x_operamini_phone_ua;

        if (argc >= 2)
        {
            user_agent.data = argv[1];
            user_agent.size = strlen(user_agent.data);
        }

        if (argc >= 3)
        {
            profile.data = argv[2];
            profile.size = strlen(profile.data);
        }

        if (argc >= 4)
        {
            x_operamini_phone_ua.data = argv[3];
            x_operamini_phone_ua.size = strlen(x_operamini_phone_ua.data);
        }

        std::string user_agent_str(user_agent.data, user_agent.size);
        user_agent_str = Poco::toLower(user_agent_str);

        UATraits::MatchedSubstrings matched_substrings;
        UATraits::Result result;
        uatraits.detect(user_agent_str, profile, x_operamini_phone_ua, result, matched_substrings);

        result.dump(std::cerr);
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}
