#pragma once

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/Config/ConfigProcessor.h>
#include <Core/Types.h>
#include <base/UUID.h>
#include <memory>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>


inline void initLogger()
{
    Poco::AutoPtr<Poco::FormattingChannel> log(
        new Poco::FormattingChannel(new Poco::PatternFormatter("%H:%M:%F [%p] %t"), new Poco::ConsoleChannel));

    Poco::Logger::root().setChannel(log);
    Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
}

inline std::shared_ptr<DB::MetadataStoreFoundationDB> createMetadataStore(const char * config_path)
{
    DB::ConfigProcessor config_processor(config_path, false, true);
    auto config = config_processor.loadConfig().configuration;
    return std::make_shared<DB::MetadataStoreFoundationDB>(*config, "foundationdb", DB::UUID{DB::UInt128{1, 1}});
}

inline const char * getConfigFile()
{
    return getenv("CONFIG_FILE");
}
