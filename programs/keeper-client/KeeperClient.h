#pragma once

#include <Client/LineReader.h>
#include <Poco/Util/Application.h>
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>

#include <iostream>

#if USE_SSL
#    include <base/extended_types.h>
#    include <optional>
#endif

namespace DB
{

class KeeperClient: public Poco::Util::Application, public KeeperClientBase
{
public:
    KeeperClient() : KeeperClientBase(std::cout, std::cerr) {}

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & args) override;

    void defineOptions(Poco::Util::OptionSet & options) override;

    String executeFourLetterCommand(const String & command) final;

protected:
    void runInteractive();
    void runInteractiveReplxx();
    void runInteractiveInputStream();

    void connectToKeeper();

    bool processQueryText(const String & text, bool is_interactive);

    std::vector<String> getCompletions(const String & prefix) const;

    zkutil::ZooKeeperArgs zk_args;

#if USE_SSL
    /// Fingerprint of the TLS material the current SSLManager client context was built from.
    /// Unset means no context has been built in this process yet.
    std::optional<UInt128> ssl_material_fingerprint;
#endif

    String history_file;
    UInt32 history_max_entries = 0; /// Maximum number of entries in the history file.

    LineReader::Suggest suggest;
};

}
