#pragma once

#include <Parsers/IParserBase.h>

#include <memory>
#include <unordered_set>

#include <boost/noncopyable.hpp>


namespace DB
{

/// The only purpose of this registry is that system tables can iterate over all parsers and call IParserBase::getDocumentation()
class ParserRegistry : private boost::noncopyable
{
public:
    static ParserRegistry & instance();

    using Creator = std::unique_ptr<IParserBase> (*)();

    void registerParser(Creator creator);

    template <typename Parser>
    void registerParser()
    {
        registerParser([]() -> std::unique_ptr<IParserBase> { return std::make_unique<Parser>(); });
    }

    const std::unordered_set<Creator> & getCreators() const { return creators; }

private:
    std::unordered_set<Creator> creators;
};

}
