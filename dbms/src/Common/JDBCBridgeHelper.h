#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/IXDBCBridgeHelper.h>

namespace DB
{
class JDBCBridgeHelper final : public IXDBCBridgeHelper
{
private:

    Poco::Logger * log = &Poco::Logger::get("JDBCBridgeHelper");

public:

    JDBCBridgeHelper(const Configuration & config_, const Poco::Timespan & http_timeout_, const std::string & connection_string_);

private:

    /* Contains logic for instantiation of the bridge instance */
    void startBridge() const override;
};

}